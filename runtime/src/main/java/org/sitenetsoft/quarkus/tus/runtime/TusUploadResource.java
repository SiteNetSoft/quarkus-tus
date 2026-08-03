package org.sitenetsoft.quarkus.tus.runtime;

import io.smallrye.mutiny.Uni;
import jakarta.enterprise.event.Event;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.SecurityContext;
import org.eclipse.microprofile.openapi.annotations.Operation;
import org.eclipse.microprofile.openapi.annotations.responses.APIResponse;
import org.eclipse.microprofile.openapi.annotations.tags.Tag;
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
@jakarta.ws.rs.Path(TusUploadResource.TUS_PATH)
@Tag(name = "TUS Upload", description = "TUS v1.0.0 resumable upload protocol")
public class TusUploadResource {

    /**
     * The path the TUS endpoints are actually mounted at. JAX-RS requires a compile-time
     * constant here, so {@code quarkus.tus.path} cannot move the endpoints; a build step
     * rejects any other configured value rather than letting the two drift apart.
     */
    public static final String TUS_PATH = "/tus";

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
    @Operation(summary = "TUS capability discovery")
    @APIResponse(responseCode = "204", description = "Server capabilities returned in headers")
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
    @Operation(summary = "Query upload status")
    @APIResponse(responseCode = "200", description = "Upload status returned in headers")
    @APIResponse(responseCode = "404", description = "Upload not found")
    @APIResponse(responseCode = "410", description = "Upload expired")
    public Response head(
            @PathParam("uploadID") String uploadID,
            @HeaderParam("Tus-Resumable") String tusResumable,
            @Context SecurityContext securityContext) {

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
        if (infoOpt.isEmpty() || isOwnershipDenied(uploadID, getCurrentUserId(securityContext))) {
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

        // Auto-finalize unfinished concatenation if all partials are now complete
        if (info.isFinalConcat() && uploadStore.isConcatReady(uploadID)) {
            if (uploadStore.finalizeConcatenation(uploadID)) {
                Optional<UploadInfo> refreshed = uploadStore.findUploadInfo(uploadID);
                if (refreshed.isPresent()) {
                    info = refreshed.get();
                }
            }
        }

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
    @Operation(summary = "Create upload or concatenation")
    @APIResponse(responseCode = "201", description = "Upload created")
    @APIResponse(responseCode = "400", description = "Invalid request")
    @APIResponse(responseCode = "413", description = "Upload size exceeds maximum")
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
            if (ids.length != parts.length) {
                return Response.status(BAD_REQUEST)
                        .header("Tus-Resumable", tusRuntimeConfig.version())
                        .entity("Upload-Concat references an invalid partial upload")
                        .build();
            }

            String currentUserId = getCurrentUserId(securityContext);

            // Validate every referenced partial up front so that an ownership failure
            // cannot fall through to a merge path that skips the check. Denied and
            // missing partials share one response so it cannot be used to probe for
            // other users' upload IDs.
            boolean allPartialsComplete = true;
            for (String partialId : ids) {
                Optional<UploadInfo> partialOpt = uploadStore.findUploadInfo(partialId);
                if (partialOpt.isEmpty() || isOwnershipDenied(partialId, currentUserId)) {
                    return mergeFailureResponse();
                }
                UploadInfo partial = partialOpt.get();
                if (!partial.isPartial() || partial.getEntityLength() < 0) {
                    return mergeFailureResponse();
                }
                if (partial.getOffset() != partial.getEntityLength()) {
                    allPartialsComplete = false;
                }
            }

            if (allPartialsComplete) {
                Optional<String> locationOpt = uploadStore.mergePartialUploadsWithOwnership(
                        ids, uploadMetadataHeader, currentUserId, value);

                if (locationOpt.isEmpty()) {
                    return mergeFailureResponse();
                }

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

            Optional<String> unfinishedOpt = uploadStore.mergePartialUploadsUnfinished(
                    ids, uploadMetadataHeader, currentUserId, value);
            if (unfinishedOpt.isPresent()) {
                return Response.status(CREATED)
                        .header("Tus-Resumable", tusRuntimeConfig.version())
                        .header("Location", unfinishedOpt.get())
                        .build();
            }

            return mergeFailureResponse();
        }

        // Validate metadata
        if (uploadMetadata != null && TusUtils.parseMetadata(uploadMetadata).isEmpty()) {
            return Response.status(BAD_REQUEST)
                    .header("Tus-Resumable", tusRuntimeConfig.version())
                    .entity("Invalid Upload-Metadata header")
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

            if (body.length > uploadSize) {
                uploadStore.discardUpload(uploadId);
                return Response.status(REQUEST_ENTITY_TOO_LARGE)
                        .header("Tus-Resumable", tusRuntimeConfig.version())
                        .entity("Body exceeds declared Upload-Length")
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
            if (infoOpt.isPresent()
                    && infoOpt.get().getOffset() == infoOpt.get().getEntityLength()
                    && infoOpt.get().markCompletionFired()) {
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
    @Operation(summary = "Upload a chunk of data")
    @APIResponse(responseCode = "204", description = "Chunk accepted")
    @APIResponse(responseCode = "404", description = "Upload not found")
    @APIResponse(responseCode = "409", description = "Offset mismatch")
    @APIResponse(responseCode = "413", description = "Chunk size exceeds maximum")
    @APIResponse(responseCode = "423", description = "Upload locked")
    @APIResponse(responseCode = "460", description = "Checksum mismatch")
    public Uni<Response> patch(
            @PathParam("uploadID") String uploadID,
            @HeaderParam("Tus-Resumable") String tusResumable,
            @HeaderParam("Upload-Offset") Long uploadOffset,
            @HeaderParam("Content-Length") Long contentLengthHeader,
            @HeaderParam("Upload-Checksum") String uploadChecksum,
            @HeaderParam("Upload-Length") Long uploadLength,
            @Context io.vertx.ext.web.RoutingContext routingContext,
            @Context SecurityContext securityContext,
            byte[] body
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
        if (uploadInfoOpt.isEmpty() || isOwnershipDenied(uploadID, getCurrentUserId(securityContext))) {
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

        // TUS spec: PATCH on a final upload URL is forbidden, whether or not the
        // concatenation has been finalized yet (isFinalConcat means "merge still pending").
        if (info.getUploadConcatMergedValue() != null || info.isFinalConcat()) {
            return Uni.createFrom().item(
                    Response.status(FORBIDDEN)
                            .header("Tus-Resumable", tusRuntimeConfig.version())
                            .entity("Cannot patch a final concatenated upload")
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

        // Parsed before the lock is taken so that a rejection cannot leak it. A blank header
        // is treated as absent; a present-but-unparseable one is a client error rather than
        // something to silently skip, since the client believes its data is being verified.
        Optional<UploadInfo.ChecksumInfo> checksumInfo = Optional.empty();
        if (uploadChecksum != null && !uploadChecksum.isBlank()) {
            checksumInfo = TusUtils.parseChecksumHeader(uploadChecksum);
            if (checksumInfo.isEmpty()) {
                return Uni.createFrom().item(
                        Response.status(BAD_REQUEST)
                                .header("Tus-Resumable", tusRuntimeConfig.version())
                                .entity("Malformed Upload-Checksum header")
                                .build()
                );
            }
            if (!isSupportedChecksumAlgorithm(checksumInfo.get().getAlgorithm())) {
                return Uni.createFrom().item(
                        Response.status(BAD_REQUEST)
                                .header("Tus-Resumable", tusRuntimeConfig.version())
                                .entity("Unsupported checksum algorithm")
                                .build()
                );
            }
        }

        if (!uploadStore.acquireLock(uploadID)) {
            return Uni.createFrom().item(
                    Response.status(423)
                            .header("Tus-Resumable", tusRuntimeConfig.version())
                            .entity("Upload is currently being processed")
                            .build()
            );
        }

        // Everything from here until the write is handed off runs under the lock. Validating
        // the offset outside it allowed two requests to both pass validation and then write in
        // turn, the second silently overwriting the first. The flag hands lock ownership to
        // the async chain, whose eventually() releases it; every earlier exit releases here.
        boolean releaseLockOnExit = true;
        try {
            Optional<UploadInfo> lockedInfoOpt = uploadStore.findUploadInfo(uploadID);
            if (lockedInfoOpt.isEmpty()) {
                return Uni.createFrom().item(
                        Response.status(NOT_FOUND)
                                .header("Tus-Resumable", tusRuntimeConfig.version())
                                .build()
                );
            }
            info = lockedInfoOpt.get();

            if (uploadLength != null && uploadStore.hasDeferredLength(uploadID)) {
                if (!uploadStore.setDeferredLength(uploadID, uploadLength)) {
                    return Uni.createFrom().item(
                            Response.status(BAD_REQUEST)
                                    .header("Tus-Resumable", tusRuntimeConfig.version())
                                    .entity("Failed to set upload length")
                                    .build()
                    );
                }
                info = uploadStore.findUploadInfo(uploadID).orElse(info);
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

            if (!checkContentLengthWithCurrentOffset(actualChunkSize, uploadOffset, info.getEntityLength())) {
                return Uni.createFrom().item(
                        Response.status(CONFLICT)
                                .header("Tus-Resumable", tusRuntimeConfig.version())
                                .entity("Chunk exceeds declared upload size")
                                .build()
                );
            }

            Uni<Response> result = writeChunkUnderLock(
                    uploadID, uploadOffset, chunk, actualChunkSize, checksumInfo, info);
            releaseLockOnExit = false;
            return result;
        } finally {
            if (releaseLockOnExit) {
                uploadStore.releaseLock(uploadID);
            }
        }
    }

    /**
     * Performs the write and builds the response. The caller must hold the upload's lock;
     * ownership passes to the returned pipeline, which releases it on termination.
     */
    private Uni<Response> writeChunkUnderLock(String uploadID,
                                              long uploadOffset,
                                              byte[] chunk,
                                              long actualChunkSize,
                                              Optional<UploadInfo.ChecksumInfo> checksumInfo,
                                              UploadInfo info) {
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
                .onFailure(org.sitenetsoft.quarkus.tus.runtime.spi.OffsetMismatchException.class)
                .recoverWithItem(e -> {
                    long expected = ((org.sitenetsoft.quarkus.tus.runtime.spi.OffsetMismatchException) e)
                            .getExpectedOffset();
                    LOG.warnf("Rejected write to upload %s at stale offset: %s", uploadID, e.getMessage());
                    return Response.status(CONFLICT)
                            .header("Tus-Resumable", tusRuntimeConfig.version())
                            .header("Upload-Offset", String.valueOf(expected))
                            .entity("Upload offset mismatch")
                            .build();
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
    @Operation(summary = "Terminate and delete upload")
    @APIResponse(responseCode = "204", description = "Upload terminated")
    public Response delete(
            @PathParam("uploadID") String uploadID,
            @HeaderParam("Tus-Resumable") String tusResumable,
            @Context SecurityContext securityContext
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

        if (isOwnershipDenied(uploadID, getCurrentUserId(securityContext))) {
            return Response.status(NOT_FOUND)
                    .header("Tus-Resumable", tusRuntimeConfig.version())
                    .build();
        }

        boolean existed = uploadStore.findUploadInfo(uploadID).isPresent();
        boolean deleted = uploadStore.discardUpload(uploadID);

        // Deleting something that was never there stays idempotent, but refusing to delete an
        // upload that exists means a write holds its lock — the client should retry.
        if (!deleted && existed) {
            return Response.status(423)
                    .header("Tus-Resumable", tusRuntimeConfig.version())
                    .entity("Upload is currently being processed")
                    .build();
        }

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

    /**
     * Whether {@code currentUserId} may not act on the given upload. Uploads with no
     * recorded uploader (created while auth was disabled) stay accessible so that
     * enabling auth does not strand them.
     */
    private boolean isOwnershipDenied(String uploadID, String currentUserId) {
        if (!tusBuildTimeConfig.authEnabled()) {
            return false;
        }
        String ownerId = uploadStore.getUploaderId(uploadID);
        return ownerId != null && !ownerId.equals(currentUserId);
    }

    private boolean isSupportedChecksumAlgorithm(String algorithm) {
        if (algorithm == null || algorithm.isBlank()) {
            return false;
        }
        for (String supported : tusRuntimeConfig.checksumAlgorithms().split(",")) {
            if (supported.trim().equalsIgnoreCase(algorithm.trim())) {
                return true;
            }
        }
        return false;
    }

    private Response mergeFailureResponse() {
        return Response.status(BAD_REQUEST)
                .header("Tus-Resumable", tusRuntimeConfig.version())
                .entity("Failed to merge partial uploads - ensure all partials exist")
                .build();
    }
}
