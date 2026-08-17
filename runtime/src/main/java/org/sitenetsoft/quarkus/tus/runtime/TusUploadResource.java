package org.sitenetsoft.quarkus.tus.runtime;

import io.smallrye.common.annotation.Blocking;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import io.vertx.ext.web.RoutingContext;
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
import org.sitenetsoft.quarkus.tus.runtime.config.TusRuntimeConfig;
import org.sitenetsoft.quarkus.tus.runtime.event.*;
import org.sitenetsoft.quarkus.tus.runtime.model.UploadInfo;
import org.sitenetsoft.quarkus.tus.runtime.model.UploadProgress;
import org.sitenetsoft.quarkus.tus.runtime.spi.OffsetMismatchException;
import org.sitenetsoft.quarkus.tus.runtime.spi.UploadNotFoundException;
import org.sitenetsoft.quarkus.tus.runtime.spi.UploadStore;
import org.sitenetsoft.quarkus.tus.runtime.spi.UploadStoreException;
import org.sitenetsoft.quarkus.tus.runtime.sse.TusSseService;

import java.security.MessageDigest;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static jakarta.ws.rs.core.Response.Status.*;

/**
 * The TUS endpoints. This class owns the protocol: it validates every rule, decides what each
 * request means, and fires the lifecycle events. The {@link UploadStore} underneath only keeps
 * records and moves bytes.
 * <p>
 * Request bodies are never materialised. POST and PATCH declare no body parameter, which makes
 * Quarkus REST leave the request paused; {@link ChunkStream} then drains it straight into the
 * store's {@code stageChunk} with backpressure, and the framework commits or aborts the staged
 * bytes once it knows whether the checksum matched and the limits held.
 */
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

    private static final String OFFSET_OCTET_STREAM = "application/offset+octet-stream";

    @Inject
    TusRuntimeConfig tusRuntimeConfig;

    @Inject
    UploadStore uploadStore;

    @Inject
    TusUploadAuthorizer authorizer;

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

    // HEAD and POST call the store's synchronous record methods, which the bundled store backs
    // with file I/O; running them on a worker keeps that off the event loop, as it was when these
    // methods returned a plain Response. The streaming chains they return work from either kind
    // of thread.
    @HEAD
    @Blocking
    @jakarta.ws.rs.Path("/{uploadID}")
    @Operation(summary = "Query upload status")
    @APIResponse(responseCode = "200", description = "Upload status returned in headers")
    @APIResponse(responseCode = "404", description = "Upload not found")
    @APIResponse(responseCode = "410", description = "Upload expired")
    public Uni<Response> head(
            @PathParam("uploadID") String uploadID,
            @HeaderParam("Tus-Resumable") String tusResumable,
            @Context SecurityContext securityContext) {

        if (tusResumable == null || !tusResumable.equals(tusRuntimeConfig.version())) {
            return Uni.createFrom().item(Response.status(412)
                    .header("Tus-Version", tusRuntimeConfig.version())
                    .build());
        }

        if (!TusUtils.isValidUuid(uploadID)) {
            return Uni.createFrom().item(tus(BAD_REQUEST).entity("Invalid upload ID format").build());
        }

        Optional<UploadInfo> infoOpt = uploadStore.findUploadInfo(uploadID);
        if (infoOpt.isEmpty() || isOwnershipDenied(uploadID, getCurrentUserId(securityContext))) {
            return Uni.createFrom().item(tus(NOT_FOUND).build());
        }

        if (isExpired(infoOpt.get())) {
            discard(uploadID);
            return Uni.createFrom().item(tus(410).entity("Upload has expired").build());
        }

        UploadInfo info = infoOpt.get();
        if (!info.isFinalConcat()) {
            return Uni.createFrom().item(headResponse(info));
        }

        // Auto-finalize an unfinished concatenation once every partial is complete.
        return finalizeConcatenationIfReady(uploadID, info)
                .onItem().transform(finalized -> headResponse(
                        uploadStore.findUploadInfo(uploadID).orElse(info)))
                .onFailure().recoverWithItem(e -> {
                    LOG.errorf(e, "Failed to finalize concatenation %s during HEAD", uploadID);
                    return headResponse(uploadStore.findUploadInfo(uploadID).orElse(info));
                });
    }

    private Response headResponse(UploadInfo info) {
        Response.ResponseBuilder builder = tus(OK)
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
        String expires = expiresHeader(info);
        if (expires != null) {
            builder.header("Upload-Expires", expires);
        }
        return builder.build();
    }

    // ---------- POST: create upload or final concat ----------

    @POST
    @Blocking
    @Operation(summary = "Create upload or concatenation")
    @APIResponse(responseCode = "201", description = "Upload created")
    @APIResponse(responseCode = "400", description = "Invalid request")
    @APIResponse(responseCode = "413", description = "Upload size exceeds maximum")
    public Uni<Response> postCreate(
            @HeaderParam("Tus-Resumable") String tusResumable,
            @HeaderParam("Upload-Length") Long uploadLength,
            @HeaderParam("Upload-Concat") String uploadConcat,
            @HeaderParam("Upload-Metadata") String uploadMetadata,
            @HeaderParam("Upload-Defer-Length") Integer uploadDeferLength,
            @HeaderParam("Content-Length") Long contentLength,
            @Context SecurityContext securityContext,
            @Context RoutingContext routingContext
    ) {
        AtomicReference<ChunkStream> stream = new AtomicReference<>();
        Uni<Response> result;
        try {
            result = doPost(tusResumable, uploadLength, uploadConcat, uploadMetadata, uploadDeferLength,
                    contentLength, securityContext, routingContext, stream);
        } catch (RuntimeException e) {
            result = Uni.createFrom().failure(e);
        }
        return result
                .onFailure().recoverWithItem(e -> {
                    LOG.error("Error while creating upload", e);
                    return tus(INTERNAL_SERVER_ERROR).entity("Internal server error").build();
                })
                .eventually(() -> discardUnreadBody(routingContext, contentLength, stream.get()));
    }

    private Uni<Response> doPost(String tusResumable, Long uploadLength, String uploadConcat,
                                 String uploadMetadata, Integer uploadDeferLength, Long contentLength,
                                 SecurityContext securityContext, RoutingContext routingContext,
                                 AtomicReference<ChunkStream> stream) {
        if (tusResumable == null || !tusResumable.equals(tusRuntimeConfig.version())) {
            return Uni.createFrom().item(Response.status(412)
                    .header("Tus-Version", tusRuntimeConfig.version())
                    .entity("Tus-Resumable header is required and must be " + tusRuntimeConfig.version())
                    .build());
        }

        String concatHeader = uploadConcat == null ? "" : uploadConcat;
        boolean isPartial = "partial".equals(concatHeader);
        boolean isDeferredLength = uploadDeferLength != null && uploadDeferLength == 1;
        String currentUserId = getCurrentUserId(securityContext);

        if (concatHeader.startsWith("final;")) {
            return createFinalConcat(uploadConcat, uploadMetadata, currentUserId);
        }

        if (uploadMetadata != null && TusUtils.parseMetadata(uploadMetadata).isEmpty()) {
            return Uni.createFrom().item(tus(BAD_REQUEST).entity("Invalid Upload-Metadata header").build());
        }

        if (uploadLength == null && !isDeferredLength) {
            return Uni.createFrom().item(tus(BAD_REQUEST)
                    .entity("Upload-Length or Upload-Defer-Length: 1 is required").build());
        }

        long uploadSize = isDeferredLength ? -1 : uploadLength;
        if (!isDeferredLength && (uploadSize < 0 || uploadSize > tusRuntimeConfig.maxSize())) {
            return Uni.createFrom().item(tus(REQUEST_ENTITY_TOO_LARGE).build());
        }

        UploadInfo info = UploadRecords.newUpload(uploadSize, uploadMetadata, isPartial, isDeferredLength,
                currentUserId, tusRuntimeConfig.expirationHours());
        String uploadId;
        try {
            uploadId = uploadStore.createUpload(info);
        } catch (UploadStoreException e) {
            LOG.error("Store failed to create upload", e);
            return Uni.createFrom().item(tus(INTERNAL_SERVER_ERROR).build());
        }

        if (!isDeferredLength) {
            uploadProgressService.startUpload(uploadId, uploadSize);
        }

        uploadCreatedEvent.fire(new TusUploadCreatedEvent(
                uploadId, uploadSize, isDeferredLength, isPartial, uploadMetadata));

        // A zero-length upload is complete the moment it exists; there will never be a chunk
        // to make the transition.
        if (!isDeferredLength && uploadSize == 0) {
            fireCompleted(uploadId, info);
        }

        String location = TUS_PATH + "/" + uploadId;
        String expires = expiresHeader(info);

        // Without a Content-Length, HTTP/1.1 announces a body with Transfer-Encoding: chunked;
        // HTTP/2 has no such header — a streamed body is just DATA frames — so there the
        // creation-with-upload content type is what tells a body from a plain creation.
        String contentTypeHeader = routingContext.request().getHeader("Content-Type");
        boolean hasBody = contentLength != null ? contentLength > 0
                : "chunked".equalsIgnoreCase(routingContext.request().getHeader("Transfer-Encoding"))
                || (routingContext.request().version() == io.vertx.core.http.HttpVersion.HTTP_2
                        && contentTypeHeader != null && contentTypeHeader.startsWith(OFFSET_OCTET_STREAM));

        if (!hasBody || isDeferredLength) {
            return Uni.createFrom().item(createdResponse(location, expires, isDeferredLength, 0));
        }

        // Creation-with-upload: the body is the first chunk.
        String contentType = routingContext.request().getHeader("Content-Type");
        if (contentType == null || !contentType.startsWith(OFFSET_OCTET_STREAM)) {
            discard(uploadId);
            return Uni.createFrom().item(tus(BAD_REQUEST)
                    .entity("Content-Type must be " + OFFSET_OCTET_STREAM + " for creation-with-upload")
                    .build());
        }
        if (contentLength != null && contentLength > tusRuntimeConfig.maxChunkSize()) {
            discard(uploadId);
            return Uni.createFrom().item(tus(REQUEST_ENTITY_TOO_LARGE)
                    .entity("Chunk size exceeds maximum allowed size").build());
        }
        if (contentLength != null && contentLength > uploadSize) {
            discard(uploadId);
            return Uni.createFrom().item(tus(REQUEST_ENTITY_TOO_LARGE)
                    .entity("Body exceeds declared Upload-Length").build());
        }

        if (!uploadStore.acquireLock(uploadId)) {
            return Uni.createFrom().item(tus(423).entity("Upload is currently being processed").build());
        }

        return detached(writeBody(uploadId, info, 0, routingContext, null, null, contentLength, stream)
                .onItem().transform(newOffset -> createdResponse(location, expires, false, newOffset))
                .onFailure(ChunkLimitExceededException.class).recoverWithItem(e ->
                        tus(REQUEST_ENTITY_TOO_LARGE).entity(e.getMessage()).build())
                .plug(u -> recoverWriteFailures(u, uploadId))
                .eventually(() -> uploadStore.releaseLock(uploadId))
                .onItem().invoke(response -> {
                    // The client gets an error and no Location, so it will create again rather
                    // than resume: leaving the upload behind only makes work for the cleanup jobs.
                    if (response.getStatus() != CREATED.getStatusCode()) {
                        discard(uploadId);
                    }
                }));
    }

    private Response createdResponse(String location, String expires, boolean deferred, long offset) {
        Response.ResponseBuilder builder = tus(CREATED).header("Location", location);
        if (expires != null) {
            builder.header("Upload-Expires", expires);
        }
        if (deferred) {
            builder.header("Upload-Defer-Length", "1");
        }
        if (offset > 0) {
            builder.header("Upload-Offset", String.valueOf(offset));
        }
        return builder.build();
    }

    // ---------- Concatenation ----------

    private Uni<Response> createFinalConcat(String uploadConcatHeader, String uploadMetadata,
                                            String currentUserId) {
        String[] parts = uploadConcatHeader.substring("final;".length()).trim().split(" ");
        if (parts.length < 1 || (parts.length == 1 && parts[0].isEmpty())) {
            return Uni.createFrom().item(tus(BAD_REQUEST)
                    .entity("Upload-Concat final requires at least one partial upload").build());
        }

        String[] ids = TusUtils.extractPartialUploadIds(parts);
        if (ids.length != parts.length) {
            return Uni.createFrom().item(tus(BAD_REQUEST)
                    .entity("Upload-Concat references an invalid partial upload").build());
        }

        // Bounds the work one request can schedule; otherwise the only ceiling is the
        // HTTP header size limit, which is not a deliberate bound.
        if (ids.length > tusRuntimeConfig.maxConcatParts()) {
            return Uni.createFrom().item(tus(BAD_REQUEST)
                    .entity("Upload-Concat references more than " + tusRuntimeConfig.maxConcatParts()
                            + " partial uploads").build());
        }

        // Every referenced partial is validated up front. Denied and missing partials share one
        // response so it cannot be used to probe for other users' upload IDs.
        //
        // Repeating a reference is rejected rather than de-duplicated: each occurrence used to
        // be summed and copied, so one uploaded partial could be inflated into a file many times
        // its size.
        Set<String> seen = new HashSet<>();
        boolean allPartialsComplete = true;
        long totalLength = 0;
        for (String partialId : ids) {
            if (!seen.add(partialId)) {
                return Uni.createFrom().item(tus(BAD_REQUEST)
                        .entity("Upload-Concat references the same partial upload more than once").build());
            }
            Optional<UploadInfo> partialOpt = uploadStore.findUploadInfo(partialId);
            if (partialOpt.isEmpty() || isOwnershipDenied(partialId, currentUserId)) {
                return Uni.createFrom().item(mergeFailureResponse());
            }
            UploadInfo partial = partialOpt.get();
            if (!partial.isPartial() || partial.getEntityLength() < 0) {
                return Uni.createFrom().item(mergeFailureResponse());
            }
            if (partial.getOffset() != partial.getEntityLength()) {
                allPartialsComplete = false;
            }
            totalLength += partial.getEntityLength();
        }

        if (totalLength > tusRuntimeConfig.maxSize()) {
            return Uni.createFrom().item(tus(REQUEST_ENTITY_TOO_LARGE)
                    .entity("Concatenated upload would exceed the maximum size").build());
        }

        UploadInfo finalInfo = UploadRecords.newFinalConcat(totalLength, uploadMetadata, currentUserId,
                List.of(ids), uploadConcatHeader, tusRuntimeConfig.expirationHours());
        String finalId;
        try {
            finalId = uploadStore.createUpload(finalInfo);
        } catch (UploadStoreException e) {
            LOG.error("Store failed to create final upload", e);
            return Uni.createFrom().item(tus(INTERNAL_SERVER_ERROR).build());
        }

        Response created = tus(CREATED).header("Location", TUS_PATH + "/" + finalId).build();
        if (!allPartialsComplete) {
            return Uni.createFrom().item(created);
        }

        return finalizeConcatenationIfReady(finalId, finalInfo)
                .onItem().transform(finalized -> created)
                .onFailure().recoverWithItem(e -> {
                    LOG.errorf(e, "Failed to concatenate into %s", finalId);
                    discard(finalId);
                    return tus(INTERNAL_SERVER_ERROR)
                            .entity("Failed to merge partial uploads").build();
                });
    }

    /**
     * Joins the partials into {@code finalId} if every one of them is complete, under the final
     * upload's lock and every partial's lock. Resolves to {@code true} if the concatenation
     * happened, {@code false} if it is not ready or another request is doing it; fails only if
     * the store's join failed.
     */
    private Uni<Boolean> finalizeConcatenationIfReady(String finalId, UploadInfo finalInfo) {
        List<String> partialIds = finalInfo.getPartialIds();
        if (!finalInfo.isFinalConcat() || partialIds == null || partialIds.isEmpty()) {
            return Uni.createFrom().item(false);
        }
        if (!uploadStore.acquireLock(finalId)) {
            return Uni.createFrom().item(false);
        }

        List<String> locked = new ArrayList<>();
        boolean ready = true;
        for (String partialId : partialIds) {
            if (!uploadStore.acquireLock(partialId)) {
                ready = false;
                break;
            }
            locked.add(partialId);
            Optional<UploadInfo> partial = uploadStore.findUploadInfo(partialId);
            if (partial.isEmpty() || partial.get().getOffset() != partial.get().getEntityLength()) {
                ready = false;
                break;
            }
        }
        // Re-read under the lock: a concurrent HEAD may already have finalized it.
        Optional<UploadInfo> current = uploadStore.findUploadInfo(finalId);
        if (current.isEmpty() || !current.get().isFinalConcat()) {
            ready = false;
        }
        if (!ready) {
            locked.forEach(uploadStore::releaseLock);
            uploadStore.releaseLock(finalId);
            return Uni.createFrom().item(false);
        }

        UploadInfo info = current.get();
        return uploadStore.concatenate(finalId, partialIds)
                .emitOn(Infrastructure.getDefaultWorkerPool())
                .onItem().transform(v -> {
                    // The partials are discarded under the locks this request still holds, so a
                    // second final over the same partials cannot slip in between and find them
                    // half gone.
                    for (String partialId : partialIds) {
                        uploadStore.discardUpload(partialId);
                        uploadProgressService.finishUpload(partialId);
                    }
                    concatenationCompletedEvent.fire(new TusConcatenationCompletedEvent(
                            finalId, partialIds.toArray(new String[0]), info.getEntityLength(),
                            info.getMetadata(), info.getUploaderId()));
                    return true;
                })
                .eventually(() -> {
                    locked.forEach(uploadStore::releaseLock);
                    uploadStore.releaseLock(finalId);
                });
    }

    // ---------- PATCH: add bytes ----------

    @PATCH
    @jakarta.ws.rs.Path("/{uploadID}")
    @Consumes(OFFSET_OCTET_STREAM)
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
            @HeaderParam("Content-Length") Long contentLength,
            @HeaderParam("Upload-Checksum") String uploadChecksum,
            @HeaderParam("Upload-Length") Long uploadLength,
            @Context RoutingContext routingContext,
            @Context SecurityContext securityContext
    ) {
        AtomicReference<ChunkStream> stream = new AtomicReference<>();
        Uni<Response> result;
        try {
            result = doPatch(uploadID, tusResumable, uploadOffset, contentLength, uploadChecksum, uploadLength,
                    routingContext, securityContext, stream);
        } catch (RuntimeException e) {
            result = Uni.createFrom().failure(e);
        }
        return result
                .onFailure().recoverWithItem(e -> {
                    LOG.error("Error while patching upload " + uploadID, e);
                    return tus(INTERNAL_SERVER_ERROR).entity("Internal server error").build();
                })
                .eventually(() -> discardUnreadBody(routingContext, contentLength, stream.get()));
    }

    private Uni<Response> doPatch(String uploadID, String tusResumable, Long uploadOffset, Long contentLength,
                                  String uploadChecksum, Long uploadLength, RoutingContext routingContext,
                                  SecurityContext securityContext, AtomicReference<ChunkStream> stream) {
        if (tusResumable == null || !tusResumable.equals(tusRuntimeConfig.version())) {
            return Uni.createFrom().item(Response.status(412)
                    .header("Tus-Version", tusRuntimeConfig.version())
                    .entity("Tus-Resumable header is required and must be " + tusRuntimeConfig.version())
                    .build());
        }

        if (!TusUtils.isValidUuid(uploadID)) {
            return Uni.createFrom().item(tus(BAD_REQUEST).entity("Invalid upload ID format").build());
        }

        if (uploadOffset == null || uploadOffset < 0) {
            return Uni.createFrom().item(tus(BAD_REQUEST)
                    .entity("Upload-Offset header is required and must be non-negative").build());
        }

        Optional<UploadInfo> uploadInfoOpt = uploadStore.findUploadInfo(uploadID);
        if (uploadInfoOpt.isEmpty() || isOwnershipDenied(uploadID, getCurrentUserId(securityContext))) {
            return Uni.createFrom().item(tus(NOT_FOUND).build());
        }

        if (isExpired(uploadInfoOpt.get())) {
            discard(uploadID);
            return Uni.createFrom().item(tus(410).entity("Upload has expired").build());
        }

        UploadInfo info = uploadInfoOpt.get();

        // TUS spec: PATCH on a final upload URL is forbidden, whether or not the
        // concatenation has been finalized yet (isFinalConcat means "merge still pending").
        if (info.getUploadConcatMergedValue() != null || info.isFinalConcat()) {
            return Uni.createFrom().item(tus(FORBIDDEN)
                    .entity("Cannot patch a final concatenated upload").build());
        }

        if (contentLength != null && contentLength > tusRuntimeConfig.maxChunkSize()) {
            return Uni.createFrom().item(tus(REQUEST_ENTITY_TOO_LARGE)
                    .entity("Chunk size exceeds maximum allowed size").build());
        }

        // Parsed before the lock is taken so that a rejection cannot leak it. A blank header
        // is treated as absent; a present-but-unparseable one is a client error rather than
        // something to silently skip, since the client believes its data is being verified.
        // checksum-trailer would read Upload-Checksum from the request trailers here, but
        // HttpServerRequest cannot expose them until eclipse-vertx/vert.x#5253 ships. See the
        // checksum-trailer branch, which builds against a patched vertx-core.
        ChecksumInfo checksumInfo = null;
        MessageDigest digest = null;
        if (uploadChecksum != null && !uploadChecksum.isBlank()) {
            Optional<ChecksumInfo> parsed = TusUtils.parseChecksumHeader(uploadChecksum);
            if (parsed.isEmpty()) {
                return Uni.createFrom().item(tus(BAD_REQUEST).entity("Malformed Upload-Checksum header").build());
            }
            checksumInfo = parsed.get();
            Optional<MessageDigest> digestOpt = isSupportedChecksumAlgorithm(checksumInfo.algorithm())
                    ? ChunkStream.digestFor(checksumInfo.algorithm()) : Optional.empty();
            if (digestOpt.isEmpty()) {
                return Uni.createFrom().item(tus(BAD_REQUEST).entity("Unsupported checksum algorithm").build());
            }
            digest = digestOpt.get();
        }

        if (!uploadStore.acquireLock(uploadID)) {
            return Uni.createFrom().item(tus(423).entity("Upload is currently being processed").build());
        }

        // Everything from here until the write is handed off runs under the lock. Validating
        // the offset outside it allowed two requests to both pass validation and then write in
        // turn, the second silently overwriting the first. The flag hands lock ownership to
        // the async chain, whose eventually() releases it; every earlier exit releases here.
        boolean releaseLockOnExit = true;
        try {
            Optional<UploadInfo> lockedInfoOpt = uploadStore.findUploadInfo(uploadID);
            if (lockedInfoOpt.isEmpty()) {
                return Uni.createFrom().item(tus(NOT_FOUND).build());
            }
            info = lockedInfoOpt.get();

            boolean lengthStillDeferred = info.isDeferredLength() && info.getEntityLength() < 0;
            if (uploadLength != null && lengthStillDeferred) {
                if (uploadLength < 0 || uploadLength > tusRuntimeConfig.maxSize()) {
                    return Uni.createFrom().item(tus(BAD_REQUEST).entity("Failed to set upload length").build());
                }
                info.setEntityLength(uploadLength);
                info.setDeferredLength(false);
                info.setLastActivity(Instant.now());
                uploadStore.updateUploadInfo(uploadID, info);
                uploadProgressService.startUpload(uploadID, uploadLength);
                lengthStillDeferred = false;
                LOG.infof("Set deferred length for upload %s to %d", uploadID, uploadLength);
                if (uploadLength == 0) {
                    fireCompleted(uploadID, info);
                }
            }

            if (lengthStillDeferred) {
                return Uni.createFrom().item(tus(BAD_REQUEST)
                        .entity("Upload-Length must be set before uploading data").build());
            }

            if (uploadOffset != info.getOffset()) {
                return Uni.createFrom().item(tus(CONFLICT)
                        .header("Upload-Offset", String.valueOf(info.getOffset()))
                        .entity("Upload offset mismatch").build());
            }

            if (contentLength != null && !fitsWithin(contentLength, uploadOffset, info.getEntityLength())) {
                return Uni.createFrom().item(tus(CONFLICT).entity("Chunk exceeds declared upload size").build());
            }

            Uni<Response> result = patchUnderLock(uploadID, info, uploadOffset, routingContext,
                    checksumInfo, digest, contentLength, stream);
            releaseLockOnExit = false;
            return result;
        } finally {
            if (releaseLockOnExit) {
                uploadStore.releaseLock(uploadID);
            }
        }
    }

    /**
     * Writes the body and builds the response. The caller must hold the upload's lock;
     * ownership passes to the returned pipeline, which releases it on termination.
     */
    private Uni<Response> patchUnderLock(String uploadID, UploadInfo info, long uploadOffset,
                                         RoutingContext routingContext, ChecksumInfo checksumInfo,
                                         MessageDigest digest, Long contentLength,
                                         AtomicReference<ChunkStream> stream) {
        final String expires = expiresHeader(info);
        final long entityLength = info.getEntityLength();

        Uni<Long> write;
        if (contentLength != null && contentLength == 0) {
            // Nothing to store; the store never sees a zero-length chunk. Some clients poll with
            // an empty PATCH, so the progress stream still hears where the upload stands.
            write = Uni.createFrom().item(uploadOffset)
                    .emitOn(Infrastructure.getDefaultWorkerPool())
                    .onItem().invoke(offset -> {
                        chunkReceivedEvent.fire(new TusChunkReceivedEvent(uploadID, 0, offset, entityLength));
                        notifyProgress(uploadID, offset, entityLength);
                    });
        } else {
            write = writeBody(uploadID, info, uploadOffset, routingContext, checksumInfo, digest, contentLength, stream);
        }

        return detached(write
                .onItem().transform(newOffset -> {
                    Response.ResponseBuilder builder = tus(NO_CONTENT)
                            .header("Upload-Offset", String.valueOf(newOffset));
                    if (expires != null) {
                        builder.header("Upload-Expires", expires);
                    }
                    return builder.build();
                })
                .plug(u -> recoverWriteFailures(u, uploadID))
                .eventually(() -> uploadStore.releaseLock(uploadID)));
    }

    /**
     * Runs {@code pipeline} to its natural end even if Quarkus REST cancels the response — which
     * it does the moment the client goes away. Cancellation would skip every failure handler in
     * the write pipeline: the abort the store was promised, the events after a commit that
     * already happened, the completion. Detached, a disconnect instead surfaces as the body
     * stream's own failure and takes the ordinary abort → release path; the response simply
     * has nobody left to read it.
     */
    private static <T> Uni<T> detached(Uni<T> pipeline) {
        return Uni.createFrom().emitter(emitter -> pipeline.subscribe().with(emitter::complete, emitter::fail));
    }

    /** Maps the failures {@link #writeBody} can produce to their TUS responses. */
    private Uni<Response> recoverWriteFailures(Uni<Response> write, String uploadID) {
        return write
                .onFailure(OffsetMismatchException.class).recoverWithItem(e -> {
                    long expected = ((OffsetMismatchException) e).getExpectedOffset();
                    LOG.warnf("Rejected write to upload %s at stale offset: %s", uploadID, e.getMessage());
                    return tus(CONFLICT)
                            .header("Upload-Offset", String.valueOf(expected))
                            .entity("Upload offset mismatch").build();
                })
                .onFailure(ChecksumMismatch.class).recoverWithItem(e -> {
                    LOG.warnf("Checksum mismatch for upload %s", uploadID);
                    return tus(460).entity("Checksum mismatch").build();
                })
                .onFailure(ChunkLimitExceededException.class).recoverWithItem(e -> {
                    ChunkLimitExceededException limit = (ChunkLimitExceededException) e;
                    return limit.kind() == ChunkLimitExceededException.Kind.CHUNK_SIZE
                            ? tus(REQUEST_ENTITY_TOO_LARGE).entity("Chunk size exceeds maximum allowed size").build()
                            : tus(CONFLICT).entity("Chunk exceeds declared upload size").build();
                })
                .onFailure(UploadNotFoundException.class).recoverWithItem(e -> tus(NOT_FOUND).build())
                .onFailure().recoverWithItem(e -> {
                    LOG.error("Error while writing to upload " + uploadID, e);
                    return tus(INTERNAL_SERVER_ERROR).entity("Internal server error").build();
                });
    }

    /**
     * Streams the request body into the store at {@code offset}: stage, then commit if the
     * checksum matched or abort if it did not, then progress bookkeeping and events. Resolves
     * to the new offset. The caller holds the upload's lock and releases it afterwards; nothing
     * here does. Fails with {@link ChecksumMismatch}, {@link ChunkLimitExceededException},
     * {@link OffsetMismatchException}, {@link UploadNotFoundException} or the store's failure.
     */
    private Uni<Long> writeBody(String uploadID, UploadInfo info, long offset, RoutingContext routingContext,
                                ChecksumInfo checksumInfo, MessageDigest digest, Long contentLength,
                                AtomicReference<ChunkStream> streamRef) {
        final long entityLength = info.getEntityLength();
        long remaining = entityLength >= 0 ? entityLength - offset : Long.MAX_VALUE;
        ChunkStream stream = new ChunkStream(routingContext, digest, tusRuntimeConfig.maxChunkSize(), remaining);
        streamRef.set(stream);
        long expectedLength = contentLength != null ? contentLength : -1;

        // deferred(): a store that throws from stageChunk instead of returning a failed Uni
        // still lands in the failure path below, where the lock is released and the error mapped.
        return Uni.createFrom().deferred(() -> uploadStore.stageChunk(uploadID, offset, stream.multi(), expectedLength))
                // The limits are ours to enforce: whatever the store made of the cut-off stream —
                // wrapped it, or even reported success — the answer is what we counted.
                .onFailure().transform(e -> stream.limitExceeded() != null ? stream.limitExceeded() : e)
                .onItem().transformToUni(staged -> {
                    if (stream.limitExceeded() != null) {
                        return Uni.createFrom().<Long>failure(stream.limitExceeded());
                    }
                    if (!stream.checksumMatches(checksumInfo)) {
                        return uploadStore.abortChunk(uploadID, offset)
                                .onItem().transformToUni(v -> Uni.createFrom().<Long>failure(new ChecksumMismatch()));
                    }
                    if (staged == 0) {
                        // A length-less body that turned out empty: nothing to commit.
                        return uploadStore.abortChunk(uploadID, offset).replaceWith(offset);
                    }
                    return uploadStore.commitChunk(uploadID, offset, staged).replaceWith(offset + staged);
                })
                .onFailure(e -> !(e instanceof ChecksumMismatch) && !(e instanceof OffsetMismatchException)).call(e -> {
                    // A store that fails mid-stage may already have discarded its bytes; abort
                    // anyway so the offset is guaranteed to be where the client left it. An
                    // abort failure must not mask the original error. Not after a stale offset,
                    // though: nothing was staged, and abortChunk(offset) would tell the store to
                    // roll the upload back below where it really is.
                    return uploadStore.abortChunk(uploadID, offset)
                            .onFailure().recoverWithItem(abortErr -> {
                                LOG.warnf(abortErr, "Failed to abort staged chunk for upload %s", uploadID);
                                return null;
                            });
                })
                .emitOn(Infrastructure.getDefaultWorkerPool())
                .onItem().invoke(newOffset -> {
                    long chunkSize = newOffset - offset;
                    uploadProgressService.updateProgress(uploadID, chunkSize);
                    chunkReceivedEvent.fire(new TusChunkReceivedEvent(uploadID, chunkSize, newOffset, entityLength));
                    notifyProgress(uploadID, newOffset, entityLength);

                    // Completion is a transition, decided once and here: only the commit that
                    // reaches the declared length fires it. A later empty PATCH at the final
                    // offset does not, and neither does a restart.
                    if (offset < entityLength && newOffset == entityLength) {
                        UploadInfo current = uploadStore.findUploadInfo(uploadID).orElse(info);
                        fireCompleted(uploadID, current);
                    }
                });
    }

    /**
     * Tells the progress stream where the upload stands. Progress entries live in memory, so
     * after a restart the store knows the upload but the progress service does not — the
     * event is then built from the offset itself, or a watcher would stall on the previous
     * chunk and never see 100%.
     */
    private void notifyProgress(String uploadID, long offset, long entityLength) {
        if (!sseServiceInstance.isResolvable()) {
            return;
        }
        UploadProgress progress = uploadProgressService.getProgress(uploadID);
        if (progress == null && entityLength >= 0) {
            progress = new UploadProgress(entityLength);
            progress.uploadedBytes = offset;
        }
        if (progress != null) {
            sseServiceInstance.get().sendProgress(uploadID, progress);
        }
    }

    private void fireCompleted(String uploadId, UploadInfo info) {
        uploadProgressService.finishUpload(uploadId);
        uploadCompletedEvent.fire(new TusUploadCompletedEvent(
                uploadId, info.getEntityLength(), info.getMetadata(), info.getUploaderId()));
    }

    /** Signals a checksum mismatch inside the write pipeline; never leaves this class. */
    private static final class ChecksumMismatch extends RuntimeException {
        ChecksumMismatch() {
            super("Checksum mismatch", null, false, false);
        }
    }

    private static boolean fitsWithin(long contentLength, long offset, long entityLength) {
        try {
            return Math.addExact(contentLength, offset) <= entityLength;
        } catch (ArithmeticException e) {
            return false;
        }
    }

    /**
     * A request answered without its body having been read leaves that body paused in Vert.x,
     * and Vert.x does not drain it — a keep-alive connection would stall on the next request.
     * Small rejected bodies are read and dropped; one larger than a chunk is not worth reading,
     * so the connection is closed after the response instead.
     */
    private void discardUnreadBody(RoutingContext routingContext, Long contentLength, ChunkStream stream) {
        if (stream != null && stream.subscribed()) {
            return;
        }
        io.vertx.core.http.HttpServerRequest request = routingContext.request();
        if (request.isEnded()) {
            return;
        }
        if (contentLength != null && contentLength > tusRuntimeConfig.maxChunkSize()) {
            routingContext.response().endHandler(v -> routingContext.response().close());
            return;
        }
        request.handler(null);
        request.resume();
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
            return tus(BAD_REQUEST).entity("Invalid upload ID format").build();
        }

        if (isOwnershipDenied(uploadID, getCurrentUserId(securityContext))) {
            return tus(NOT_FOUND).build();
        }

        // Deleting something that was never there stays idempotent, but a write holding the
        // lock means the client should retry.
        Discard outcome = discard(uploadID);
        if (outcome == Discard.LOCKED) {
            return tus(423).entity("Upload is currently being processed").build();
        }

        LOG.infof("UploadID %s deleted=%s", uploadID, outcome == Discard.REMOVED);

        if (sseServiceInstance.isResolvable()) {
            sseServiceInstance.get().unregister(uploadID);
        }

        uploadTerminatedEvent.fire(new TusUploadTerminatedEvent(uploadID));

        return tus(NO_CONTENT).build();
    }

    // ---------- helpers ----------

    private enum Discard { REMOVED, ABSENT, LOCKED }

    /**
     * Discards an upload the way every path in here must: under its lock, so nothing is deleted
     * underneath an in-flight write, and with the framework's own progress bookkeeping cleared.
     */
    private Discard discard(String uploadId) {
        if (!uploadStore.acquireLock(uploadId)) {
            return Discard.LOCKED;
        }
        boolean removed;
        try {
            removed = uploadStore.discardUpload(uploadId);
        } finally {
            uploadStore.releaseLock(uploadId);
        }
        uploadProgressService.finishUpload(uploadId);
        return removed ? Discard.REMOVED : Discard.ABSENT;
    }

    private Response.ResponseBuilder tus(Response.Status status) {
        return Response.status(status).header("Tus-Resumable", tusRuntimeConfig.version());
    }

    private Response.ResponseBuilder tus(int status) {
        return Response.status(status).header("Tus-Resumable", tusRuntimeConfig.version());
    }

    private static boolean isExpired(UploadInfo info) {
        return info.getExpiresAt() != null && Instant.now().isAfter(info.getExpiresAt());
    }

    private static String expiresHeader(UploadInfo info) {
        if (info.getExpiresAt() == null) {
            return null;
        }
        return DateTimeFormatter.RFC_1123_DATE_TIME.withZone(ZoneOffset.UTC).format(info.getExpiresAt());
    }

    private String getCurrentUserId(SecurityContext securityContext) {
        return authorizer.currentUserId(securityContext);
    }

    private boolean isOwnershipDenied(String uploadID, String currentUserId) {
        return authorizer.isDenied(uploadID, currentUserId);
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
        return tus(BAD_REQUEST)
                .entity("Failed to merge partial uploads - ensure all partials exist")
                .build();
    }
}
