package org.sitenetsoft.quarkus.tus.runtime;

import io.smallrye.mutiny.Uni;
import jakarta.enterprise.event.Event;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.SecurityContext;
import org.jboss.logging.Logger;
import org.sitenetsoft.quarkus.tus.runtime.config.TusBuildTimeConfig;
import org.sitenetsoft.quarkus.tus.runtime.config.TusRuntimeConfig;
import org.sitenetsoft.quarkus.tus.runtime.event.*;
import org.sitenetsoft.quarkus.tus.runtime.model.UploadInfo;
import org.sitenetsoft.quarkus.tus.runtime.model.UploadProgress;
import org.sitenetsoft.quarkus.tus.runtime.spi.UploadStore;
import org.sitenetsoft.quarkus.tus.runtime.sse.TusSseService;
import org.sitenetsoft.quarkus.tus.runtime.store.LocalFileUploadStore;

import java.util.Optional;

import static jakarta.ws.rs.core.Response.Status.*;

@jakarta.enterprise.context.ApplicationScoped
@jakarta.ws.rs.Path("/tus")
public class TusUploadResource {

    private static final Logger LOG = Logger.getLogger(TusUploadResource.class);

    private static String extractUploadIdFromLocation(String location) {
        if (location == null || location.isBlank()) {
            return null;
        }
        int lastSlash = location.lastIndexOf('/');
        if (lastSlash >= 0 && lastSlash < location.length() - 1) {
            String id = location.substring(lastSlash + 1);
            int queryStart = id.indexOf('?');
            if (queryStart > 0) {
                id = id.substring(0, queryStart);
            }
            return id;
        }
        return location;
    }

    @Inject
    TusRuntimeConfig tusRuntimeConfig;

    @Inject
    TusBuildTimeConfig tusBuildTimeConfig;

    @Inject
    UploadStore uploadStore;

    @Inject
    Instance<TusSseService> sseServiceInstance;

    @Inject
    UploadProgressService uploadProgressService;

    @Inject
    Event<TusUploadCreatedEvent> uploadCreatedEvent;

    @Inject
    Event<TusChunkReceivedEvent> chunkReceivedEvent;

    @Inject
    Event<TusUploadTerminatedEvent> uploadTerminatedEvent;

    @Inject
    Event<TusUploadCompletedEvent> uploadCompletedEvent;

    @Inject
    Event<TusConcatenationCompletedEvent> concatenationCompletedEvent;

    // ---------- OPTIONS: server capabilities ----------

    @OPTIONS
    public Response options() {
        return Response.noContent()
                .header("Tus-Resumable", tusRuntimeConfig.version())
                .header("Tus-Version", tusRuntimeConfig.version())
                .header("Tus-Max-Size", tusRuntimeConfig.maxSize())
                .header("Tus-Extension", tusRuntimeConfig.extensions())
                .header("Tus-Checksum-Algorithm", tusRuntimeConfig.checksumAlgorithms())
                .build();
    }

    // ---------- HEAD: upload status ----------

    @HEAD
    @jakarta.ws.rs.Path("/{uploadID}")
    public Response head(
            @PathParam("uploadID") String uploadID,
            @HeaderParam("Tus-Resumable") String tusResumable) {

        if (tusResumable == null || !tusResumable.equals(tusRuntimeConfig.version())) {
            return Response.status(412)
                    .header("Tus-Version", tusRuntimeConfig.version())
                    .build();
        }

        if (!TusUtils.isValidUuid(uploadID)) {
            return Response.status(BAD_REQUEST)
                    .header("Tus-Resumable", tusRuntimeConfig.version())
                    .entity("Invalid upload ID format")
                    .build();
        }

        Optional<UploadInfo> infoOpt = uploadStore.findUploadInfo(uploadID);
        if (infoOpt.isEmpty()) {
            return Response.status(NOT_FOUND)
                    .header("Tus-Resumable", tusRuntimeConfig.version())
                    .build();
        }

        if (uploadStore.isExpired(uploadID)) {
            uploadStore.discardUpload(uploadID);
            return Response.status(410)
                    .header("Tus-Resumable", tusRuntimeConfig.version())
                    .entity("Upload has expired")
                    .build();
        }

        UploadInfo info = infoOpt.get();

        Response.ResponseBuilder builder = Response.ok()
                .header("Tus-Resumable", tusRuntimeConfig.version())
                .header("Cache-Control", "no-store")
                .header("Upload-Offset", Long.toString(info.getOffset()));

        if (info.getEntityLength() >= 0) {
            builder.header("Upload-Length", Long.toString(info.getEntityLength()));
        }

        if (info.isDeferredLength() && info.getEntityLength() < 0) {
            builder.header("Upload-Defer-Length", "1");
        }

        if (info.getMetadata() != null) {
            builder.header("Upload-Metadata", info.getMetadata());
        }

        if (info.isPartial()) {
            builder.header("Upload-Concat", "partial");
        } else if (info.getUploadConcatMergedValue() != null) {
            builder.header("Upload-Concat", info.getUploadConcatMergedValue());
        }

        if (info.getExpiresAt() != null) {
            String expiresHeader = java.time.format.DateTimeFormatter.RFC_1123_DATE_TIME
                    .withZone(java.time.ZoneOffset.UTC)
                    .format(info.getExpiresAt());
            builder.header("Upload-Expires", expiresHeader);
        }

        return builder.build();
    }

    // ---------- POST: create upload or final concat ----------

    @POST
    public Response postCreate(
            @HeaderParam("Tus-Resumable") String tusResumable,
            @HeaderParam("Upload-Length") Long uploadLength,
            @HeaderParam("Upload-Concat") String uploadConcat,
            @HeaderParam("Upload-Metadata") String uploadMetadata,
            @HeaderParam("Upload-Defer-Length") Integer uploadDeferLength,
            @Context jakarta.ws.rs.core.UriInfo uriInfo,
            @Context SecurityContext securityContext,
            @Context io.vertx.ext.web.RoutingContext routingContext,
            byte[] body
    ) {
        if (tusResumable == null || !tusResumable.equals(tusRuntimeConfig.version())) {
            return Response.status(412)
                    .header("Tus-Version", tusRuntimeConfig.version())
                    .entity("Tus-Resumable header is required and must be " + tusRuntimeConfig.version())
                    .build();
        }

        Optional<Long> lengthHeader = Optional.ofNullable(uploadLength);
        Optional<String> uploadConcatHeader = Optional.ofNullable(uploadConcat);
        Optional<String> uploadMetadataHeader = Optional.ofNullable(uploadMetadata);
        boolean isDeferredLength = uploadDeferLength != null && uploadDeferLength == 1;

        boolean isPartial = "partial".equals(uploadConcatHeader.orElse(""));
        boolean isPotentiallyFinal = uploadConcatHeader.orElse("").startsWith("final;");

        if (isPotentiallyFinal) {
            String value = uploadConcatHeader.get();
            String[] parts = value.substring("final;".length()).trim().split(" ");
            if (parts.length < 1 || (parts.length == 1 && parts[0].isEmpty())) {
                return Response.status(BAD_REQUEST)
                        .header("Tus-Resumable", tusRuntimeConfig.version())
                        .entity("Upload-Concat final requires at least one partial upload")
                        .build();
            }

            String[] ids = TusUtils.extractPartialUploadIds(parts);

            String currentUserId = getCurrentUserId(securityContext);
            Optional<String> locationOpt = uploadStore.mergePartialUploadsWithOwnership(
                    ids, uploadMetadataHeader, currentUserId);

            if (locationOpt.isPresent()) {
                String location = locationOpt.get();
                String finalUploadId = extractUploadIdFromLocation(location);

                Optional<UploadInfo> mergedInfoOpt = uploadStore.findUploadInfo(finalUploadId);
                long totalSize = mergedInfoOpt.map(UploadInfo::getEntityLength).orElse(0L);

                concatenationCompletedEvent.fire(new TusConcatenationCompletedEvent(
                        finalUploadId, ids, totalSize, uploadMetadata, currentUserId));

                return Response.status(CREATED)
                        .header("Tus-Resumable", tusRuntimeConfig.version())
                        .header("Location", location)
                        .build();
            }

            Optional<String> unfinishedOpt = uploadStore.mergePartialUploadsUnfinished(ids, uploadMetadataHeader);
            if (unfinishedOpt.isPresent()) {
                return Response.status(CREATED)
                        .header("Tus-Resumable", tusRuntimeConfig.version())
                        .header("Location", unfinishedOpt.get())
                        .build();
            }

            return Response.status(BAD_REQUEST)
                    .header("Tus-Resumable", tusRuntimeConfig.version())
                    .entity("Failed to merge partial uploads - ensure all partials exist")
                    .build();
        }

        // Normal creation
        if (lengthHeader.isEmpty() && !isDeferredLength) {
            return Response.status(BAD_REQUEST)
                    .header("Tus-Resumable", tusRuntimeConfig.version())
                    .entity("Upload-Length or Upload-Defer-Length: 1 is required")
                    .build();
        }

        Optional<String> locationOpt;
        String uploadId;
        long uploadSize;

        if (isDeferredLength) {
            locationOpt = uploadStore.createUploadDeferred(uploadMetadataHeader, isPartial);
            uploadSize = -1;
        } else {
            if (!uploadStore.checkServerSizeConstraint(lengthHeader.get())) {
                return Response.status(REQUEST_ENTITY_TOO_LARGE)
                        .header("Tus-Resumable", tusRuntimeConfig.version())
                        .build();
            }
            locationOpt = uploadStore.createUpload(lengthHeader.get(), uploadMetadataHeader, isPartial);
            uploadSize = lengthHeader.get();
        }

        if (locationOpt.isEmpty()) {
            return Response.status(INTERNAL_SERVER_ERROR)
                    .header("Tus-Resumable", tusRuntimeConfig.version())
                    .build();
        }

        String location = locationOpt.get();
        uploadId = extractUploadIdFromLocation(location);

        String currentUserId = getCurrentUserId(securityContext);
        if (currentUserId != null) {
            uploadStore.setUploaderId(uploadId, currentUserId);
        }

        Optional<java.time.Instant> expiresAt = uploadStore.getExpiresAt(uploadId);
        String expiresHeader = expiresAt
                .map(instant -> java.time.format.DateTimeFormatter.RFC_1123_DATE_TIME
                        .withZone(java.time.ZoneOffset.UTC)
                        .format(instant))
                .orElse(null);

        if (!isDeferredLength) {
            uploadProgressService.startUpload(uploadId, uploadSize);
        }

        uploadCreatedEvent.fire(new TusUploadCreatedEvent(
                uploadId, uploadSize, isDeferredLength, isPartial, uploadMetadata));

        // Handle creation-with-upload
        long initialOffset = 0;
        if (body != null && body.length > 0 && !isDeferredLength) {
            String contentType = routingContext != null
                    ? routingContext.request().getHeader("Content-Type") : null;
            if (contentType == null || !contentType.startsWith("application/offset+octet-stream")) {
                uploadStore.discardUpload(uploadId);
                return Response.status(BAD_REQUEST)
                        .header("Tus-Resumable", tusRuntimeConfig.version())
                        .entity("Content-Type must be application/offset+octet-stream for creation-with-upload")
                        .build();
            }

            if (body.length > tusRuntimeConfig.maxChunkSize()) {
                uploadStore.discardUpload(uploadId);
                return Response.status(REQUEST_ENTITY_TOO_LARGE)
                        .header("Tus-Resumable", tusRuntimeConfig.version())
                        .entity("Chunk size exceeds maximum allowed size")
                        .build();
            }

            initialOffset = uploadStore.writeInitialData(uploadId, body);
            if (initialOffset < 0) {
                return Response.status(INTERNAL_SERVER_ERROR)
                        .header("Tus-Resumable", tusRuntimeConfig.version())
                        .entity("Failed to write initial upload data")
                        .build();
            }

            uploadProgressService.updateProgress(uploadId, body.length);

            Optional<UploadInfo> infoOpt = uploadStore.findUploadInfo(uploadId);
            if (infoOpt.isPresent() && infoOpt.get().getOffset() == infoOpt.get().getEntityLength()) {
                uploadProgressService.finishUpload(uploadId);
                uploadCompletedEvent.fire(new TusUploadCompletedEvent(
                        uploadId, infoOpt.get().getEntityLength(), uploadMetadata,
                        getCurrentUserId(securityContext)));
            }
        }

        Response.ResponseBuilder responseBuilder = Response.status(CREATED)
                .header("Tus-Resumable", tusRuntimeConfig.version())
                .header("Location", location);

        if (expiresHeader != null) {
            responseBuilder.header("Upload-Expires", expiresHeader);
        }

        if (isDeferredLength) {
            responseBuilder.header("Upload-Defer-Length", "1");
        }

        if (initialOffset > 0) {
            responseBuilder.header("Upload-Offset", String.valueOf(initialOffset));
        }

        return responseBuilder.build();
    }

    // ---------- PATCH: add bytes ----------

    @PATCH
    @jakarta.ws.rs.Path("/{uploadID}")
    @Consumes("application/offset+octet-stream")
    public Uni<Response> patch(
            @PathParam("uploadID") String uploadID,
            @HeaderParam("Tus-Resumable") String tusResumable,
            @HeaderParam("Upload-Offset") Long uploadOffset,
            @HeaderParam("Content-Length") Long contentLengthHeader,
            @HeaderParam("Upload-Checksum") String uploadChecksum,
            @HeaderParam("Upload-Length") Long uploadLength,
            byte[] body,
            @Context io.vertx.ext.web.RoutingContext routingContext
    ) {
        if (tusResumable == null || !tusResumable.equals(tusRuntimeConfig.version())) {
            return Uni.createFrom().item(
                    Response.status(412)
                            .header("Tus-Version", tusRuntimeConfig.version())
                            .entity("Tus-Resumable header is required and must be " + tusRuntimeConfig.version())
                            .build()
            );
        }

        if (!TusUtils.isValidUuid(uploadID)) {
            return Uni.createFrom().item(
                    Response.status(BAD_REQUEST)
                            .header("Tus-Resumable", tusRuntimeConfig.version())
                            .entity("Invalid upload ID format")
                            .build()
            );
        }

        if (uploadOffset == null || uploadOffset < 0) {
            return Uni.createFrom().item(
                    Response.status(BAD_REQUEST)
                            .header("Tus-Resumable", tusRuntimeConfig.version())
                            .entity("Upload-Offset header is required and must be non-negative")
                            .build()
            );
        }

        Optional<UploadInfo> uploadInfoOpt = uploadStore.findUploadInfo(uploadID);
        if (uploadInfoOpt.isEmpty()) {
            return Uni.createFrom().item(
                    Response.status(NOT_FOUND)
                            .header("Tus-Resumable", tusRuntimeConfig.version())
                            .build()
            );
        }

        if (uploadStore.isExpired(uploadID)) {
            uploadStore.discardUpload(uploadID);
            return Uni.createFrom().item(
                    Response.status(410)
                            .header("Tus-Resumable", tusRuntimeConfig.version())
                            .entity("Upload has expired")
                            .build()
            );
        }

        UploadInfo info = uploadInfoOpt.get();

        // Handle deferred length
        if (uploadLength != null && uploadStore.hasDeferredLength(uploadID)) {
            if (!uploadStore.setDeferredLength(uploadID, uploadLength)) {
                return Uni.createFrom().item(
                        Response.status(BAD_REQUEST)
                                .header("Tus-Resumable", tusRuntimeConfig.version())
                                .entity("Failed to set upload length")
                                .build()
                );
            }
            Optional<UploadInfo> updatedInfo = uploadStore.findUploadInfo(uploadID);
            if (updatedInfo.isPresent()) {
                info = updatedInfo.get();
            }
        }

        if (uploadStore.hasDeferredLength(uploadID)) {
            return Uni.createFrom().item(
                    Response.status(BAD_REQUEST)
                            .header("Tus-Resumable", tusRuntimeConfig.version())
                            .entity("Upload-Length must be set before uploading data")
                            .build()
            );
        }

        if (!uploadStore.validateOffset(uploadID, uploadOffset)) {
            return Uni.createFrom().item(
                    Response.status(CONFLICT)
                            .header("Tus-Resumable", tusRuntimeConfig.version())
                            .header("Upload-Offset", String.valueOf(info.getOffset()))
                            .entity("Upload offset mismatch")
                            .build()
            );
        }

        byte[] chunk = (body != null) ? body : new byte[0];
        long actualChunkSize = chunk.length;

        if (actualChunkSize > tusRuntimeConfig.maxChunkSize()) {
            return Uni.createFrom().item(
                    Response.status(REQUEST_ENTITY_TOO_LARGE)
                            .header("Tus-Resumable", tusRuntimeConfig.version())
                            .entity("Chunk size exceeds maximum allowed size")
                            .build()
            );
        }

        if (!checkContentLengthWithCurrentOffset(actualChunkSize, uploadOffset, info.getEntityLength())) {
            return Uni.createFrom().item(
                    Response.status(CONFLICT)
                            .header("Tus-Resumable", tusRuntimeConfig.version())
                            .entity("Chunk exceeds declared upload size")
                            .build()
            );
        }

        if (!uploadStore.acquireLock(uploadID)) {
            return Uni.createFrom().item(
                    Response.status(423)
                            .header("Tus-Resumable", tusRuntimeConfig.version())
                            .entity("Upload is currently being processed")
                            .build()
            );
        }

        // Parse checksum
        Optional<UploadInfo.ChecksumInfo> checksumInfo;
        if (uploadChecksum != null) {
            checksumInfo = TusUtils.parseChecksumHeader(uploadChecksum);
        } else if (routingContext != null) {
            String trailerChecksum = null;
            Object capturedTrailers = routingContext.get("http.trailers");
            if (capturedTrailers instanceof io.vertx.core.MultiMap trailers) {
                trailerChecksum = trailers.get("Upload-Checksum");
            }
            if (trailerChecksum != null) {
                checksumInfo = TusUtils.parseChecksumHeader(trailerChecksum);
            } else {
                checksumInfo = Optional.empty();
            }
        } else {
            checksumInfo = Optional.empty();
        }

        final String expiresHeader = info.getExpiresAt() != null
                ? java.time.format.DateTimeFormatter.RFC_1123_DATE_TIME
                        .withZone(java.time.ZoneOffset.UTC)
                        .format(info.getExpiresAt())
                : null;

        final UploadInfo finalInfo = info;

        return uploadStore.writeChunkAsync(uploadID, uploadOffset, chunk, checksumInfo)
                .onItem().transform(newOffset -> {
                    if (newOffset == -1) {
                        return Response.status(NOT_FOUND)
                                .header("Tus-Resumable", tusRuntimeConfig.version())
                                .build();
                    }

                    if (actualChunkSize > 0) {
                        uploadProgressService.updateProgress(uploadID, actualChunkSize);
                    }

                    // Fire CDI event
                    chunkReceivedEvent.fire(new TusChunkReceivedEvent(
                            uploadID, actualChunkSize, newOffset, finalInfo.getEntityLength()));

                    // Send SSE progress if available
                    if (sseServiceInstance.isResolvable()) {
                        UploadProgress progress = uploadProgressService.getProgress(uploadID);
                        if (progress != null) {
                            sseServiceInstance.get().sendProgress(uploadID, progress);
                        }
                    }

                    Response.ResponseBuilder responseBuilder = Response.noContent()
                            .header("Tus-Resumable", tusRuntimeConfig.version())
                            .header("Upload-Offset", String.valueOf(newOffset));

                    if (expiresHeader != null) {
                        responseBuilder.header("Upload-Expires", expiresHeader);
                    }

                    return responseBuilder.build();
                })
                .onFailure(LocalFileUploadStore.ChecksumMismatchException.class).recoverWithItem(e -> {
                    LOG.warnf("Checksum mismatch for upload %s: %s", uploadID, e.getMessage());
                    return Response.status(460)
                            .header("Tus-Resumable", tusRuntimeConfig.version())
                            .entity("Checksum mismatch")
                            .build();
                })
                .onFailure().recoverWithItem(e -> {
                    LOG.error("Error while patching upload " + uploadID, e);
                    return Response.status(INTERNAL_SERVER_ERROR)
                            .header("Tus-Resumable", tusRuntimeConfig.version())
                            .entity("Internal server error")
                            .build();
                })
                .eventually(() -> {
                    uploadStore.releaseLock(uploadID);
                    return Uni.createFrom().voidItem();
                });
    }

    private boolean checkContentLengthWithCurrentOffset(Long contentLength, Long offset, Long entityLength) {
        if (contentLength == null) return true;
        if (offset == null || entityLength == null) return false;
        try {
            long total = Math.addExact(contentLength, offset);
            return total <= entityLength;
        } catch (ArithmeticException e) {
            return false;
        }
    }

    // ---------- DELETE: termination extension ----------

    @DELETE
    @jakarta.ws.rs.Path("/{uploadID}")
    public Response delete(
            @PathParam("uploadID") String uploadID,
            @HeaderParam("Tus-Resumable") String tusResumable
    ) {
        if (tusResumable == null || !tusResumable.equals(tusRuntimeConfig.version())) {
            return Response.status(412)
                    .header("Tus-Version", tusRuntimeConfig.version())
                    .build();
        }

        if (!TusUtils.isValidUuid(uploadID)) {
            return Response.status(BAD_REQUEST)
                    .header("Tus-Resumable", tusRuntimeConfig.version())
                    .entity("Invalid upload ID format")
                    .build();
        }

        boolean deleted = uploadStore.discardUpload(uploadID);
        LOG.infof("UploadID %s deleted=%s", uploadID, deleted);

        uploadProgressService.finishUpload(uploadID);
        if (sseServiceInstance.isResolvable()) {
            sseServiceInstance.get().unregister(uploadID);
        }

        uploadTerminatedEvent.fire(new TusUploadTerminatedEvent(uploadID));

        return Response.noContent()
                .header("Tus-Resumable", tusRuntimeConfig.version())
                .build();
    }

    private String getCurrentUserId(SecurityContext securityContext) {
        if (securityContext != null && securityContext.getUserPrincipal() != null) {
            return securityContext.getUserPrincipal().getName();
        }
        return null;
    }
}
