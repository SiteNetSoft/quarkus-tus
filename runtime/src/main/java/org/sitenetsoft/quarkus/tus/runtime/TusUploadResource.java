package org.sitenetsoft.quarkus.tus.runtime;

import io.smallrye.common.annotation.Blocking;
import io.smallrye.mutiny.Multi;
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
    UploadWriter writer;

    @Inject
    UploadConcatenator concatenator;

    @Inject
    UploadEvents events;

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

        String currentUserId = getCurrentUserId(securityContext);
        return uploadStore.findUploadInfo(uploadID).chain(infoOpt -> {
            if (infoOpt.isEmpty() || isOwnershipDenied(infoOpt.get(), currentUserId)) {
                return Uni.createFrom().item(tus(NOT_FOUND).build());
            }
            UploadInfo info = infoOpt.get();

            if (isExpired(info)) {
                return discard(uploadID).replaceWith(() -> tus(410).entity("Upload has expired").build());
            }

            if (!info.isFinalConcat()) {
                return Uni.createFrom().item(headResponse(info));
            }

            // Auto-finalize an unfinished concatenation once every partial is complete.
            return concatenator.finalizeIfReady(uploadID, info)
                    .onFailure().recoverWithItem(e -> {
                        LOG.errorf(e, "Failed to finalize concatenation %s during HEAD", uploadID);
                        return false;
                    })
                    .chain(finalized -> uploadStore.findUploadInfo(uploadID))
                    .map(current -> headResponse(current.orElse(info)));
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

    // No @Blocking anywhere: every store method is asynchronous, so no request thread waits on
    // storage. What still needs a worker is application code — the lifecycle events — and the
    // chains hop onto one before firing them.
    @POST
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
        final long size = uploadSize;
        final boolean deferred = isDeferredLength;
        final boolean partial = isPartial;

        return uploadStore.createUpload(info)
                .onFailure().recoverWithUni(e -> {
                    LOG.error("Store failed to create upload", e);
                    return Uni.createFrom().nullItem();
                })
                .chain(uploadId -> uploadId == null
                        ? Uni.createFrom().item(tus(INTERNAL_SERVER_ERROR).build())
                        : afterCreate(uploadId, info, size, deferred, partial, uploadMetadata,
                                contentLength, routingContext, stream));
    }

    /**
     * The part of creation that follows the store having a record: events, then the optional
     * body. Runs on a worker — the events reach application code, which may do anything — and
     * inside the chain, so an observer has run before the client sees the Location.
     */
    private Uni<Response> afterCreate(String uploadId, UploadInfo info, long uploadSize, boolean isDeferredLength,
                                      boolean isPartial, String uploadMetadata, Long contentLength,
                                      RoutingContext routingContext, AtomicReference<ChunkStream> stream) {
        return Uni.createFrom().voidItem()
                .emitOn(Infrastructure.getDefaultWorkerPool())
                .chain(() -> announceAndWrite(uploadId, info, uploadSize, isDeferredLength, isPartial,
                        uploadMetadata, contentLength, routingContext, stream));
    }

    private Uni<Response> announceAndWrite(String uploadId, UploadInfo info, long uploadSize,
                                           boolean isDeferredLength, boolean isPartial, String uploadMetadata,
                                           Long contentLength, RoutingContext routingContext,
                                           AtomicReference<ChunkStream> stream) {
        events.uploadCreated(uploadId, uploadSize, isDeferredLength, isPartial, uploadMetadata);

        // A zero-length upload is complete the moment it exists; there will never be a chunk
        // to make the transition.
        if (!isDeferredLength && uploadSize == 0) {
            events.uploadCompleted(uploadId, info);
        }

        String location = TUS_PATH + "/" + uploadId;
        String expires = expiresHeader(info);

        // Without a Content-Length, HTTP/1.1 announces a body with Transfer-Encoding: chunked;
        // HTTP/2 has no such header — a streamed body is just DATA frames — so there the
        // creation-with-upload content type is what tells a body from a plain creation.
        String contentType = routingContext.request().getHeader("Content-Type");
        boolean hasBody = contentLength != null ? contentLength > 0
                : "chunked".equalsIgnoreCase(routingContext.request().getHeader("Transfer-Encoding"))
                || (routingContext.request().version() == io.vertx.core.http.HttpVersion.HTTP_2
                        && contentType != null && contentType.startsWith(OFFSET_OCTET_STREAM));

        if (!hasBody || isDeferredLength) {
            return Uni.createFrom().item(createdResponse(location, expires, isDeferredLength, 0));
        }

        // Creation-with-upload: the body is the first chunk.
        if (contentType == null || !contentType.startsWith(OFFSET_OCTET_STREAM)) {
            return discard(uploadId).replaceWith(() -> tus(BAD_REQUEST)
                    .entity("Content-Type must be " + OFFSET_OCTET_STREAM + " for creation-with-upload")
                    .build());
        }
        if (contentLength != null && contentLength > tusRuntimeConfig.maxChunkSize()) {
            return discard(uploadId).replaceWith(() -> tus(REQUEST_ENTITY_TOO_LARGE)
                    .entity("Chunk size exceeds maximum allowed size").build());
        }
        if (contentLength != null && contentLength > uploadSize) {
            return discard(uploadId).replaceWith(() -> tus(REQUEST_ENTITY_TOO_LARGE)
                    .entity("Body exceeds declared Upload-Length").build());
        }

        return uploadStore.acquireLock(uploadId).chain(locked -> {
            if (!locked) {
                return Uni.createFrom().item(tus(423).entity("Upload is currently being processed").build());
            }
            return detached(writer.write(uploadId, info, 0, routingContext, null, null, contentLength, stream)
                    .onItem().transform(newOffset -> createdResponse(location, expires, false, newOffset))
                    .onFailure(ChunkLimitExceededException.class).recoverWithItem(e ->
                            tus(REQUEST_ENTITY_TOO_LARGE).entity(e.getMessage()).build())
                    .plug(u -> recoverWriteFailures(u, uploadId))
                    .eventually(() -> uploadStore.releaseLock(uploadId))
                    // The client gets an error and no Location, so it will create again rather
                    // than resume: leaving the upload behind only makes work for the cleanup jobs.
                    .call(response -> response.getStatus() == CREATED.getStatusCode()
                            ? Uni.createFrom().voidItem()
                            : discard(uploadId).replaceWithVoid()));
        });
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
        for (String partialId : ids) {
            if (!seen.add(partialId)) {
                return Uni.createFrom().item(tus(BAD_REQUEST)
                        .entity("Upload-Concat references the same partial upload more than once").build());
            }
        }

        return Multi.createFrom().iterable(List.of(ids))
                .onItem().transformToUniAndConcatenate(uploadStore::findUploadInfo)
                .collect().asList()
                .chain(partials -> {
                    boolean allPartialsComplete = true;
                    long totalLength = 0;
                    for (Optional<UploadInfo> partialOpt : partials) {
                        if (partialOpt.isEmpty() || isOwnershipDenied(partialOpt.get(), currentUserId)) {
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
                    final boolean ready = allPartialsComplete;
                    return uploadStore.createUpload(finalInfo)
                            .onFailure().recoverWithUni(e -> {
                                LOG.error("Store failed to create final upload", e);
                                return Uni.createFrom().nullItem();
                            })
                            .chain(finalId -> {
                                if (finalId == null) {
                                    return Uni.createFrom().item(tus(INTERNAL_SERVER_ERROR).build());
                                }
                                Response created = tus(CREATED).header("Location", TUS_PATH + "/" + finalId).build();
                                if (!ready) {
                                    return Uni.createFrom().item(created);
                                }
                                return concatenator.finalizeIfReady(finalId, finalInfo)
                                        .replaceWith(created)
                                        .onFailure().recoverWithUni(e -> {
                                            LOG.errorf(e, "Failed to concatenate into %s", finalId);
                                            return discard(finalId).replaceWith(() -> tus(INTERNAL_SERVER_ERROR)
                                                    .entity("Failed to merge partial uploads").build());
                                        });
                            });
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

        String currentUserId = getCurrentUserId(securityContext);
        final long offset = uploadOffset;
        return uploadStore.findUploadInfo(uploadID).chain(uploadInfoOpt -> {
            if (uploadInfoOpt.isEmpty() || isOwnershipDenied(uploadInfoOpt.get(), currentUserId)) {
                return Uni.createFrom().item(tus(NOT_FOUND).build());
            }

            UploadInfo info = uploadInfoOpt.get();

            if (isExpired(info)) {
                return discard(uploadID).replaceWith(() -> tus(410).entity("Upload has expired").build());
            }

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

            final ChecksumInfo expectedChecksum = checksumInfo;
            final MessageDigest bodyDigest = digest;
            return uploadStore.acquireLock(uploadID).chain(locked -> {
                if (!locked) {
                    return Uni.createFrom().item(tus(423).entity("Upload is currently being processed").build());
                }
                // From here the lock is held, and every path below releases it: the rejections
                // through releasingLock, the write through patchUnderLock's eventually. Validating
                // the offset outside the lock allowed two requests to both pass validation and
                // then write in turn, the second silently overwriting the first.
                return patchUnderLockValidated(uploadID, offset, contentLength, uploadLength,
                        expectedChecksum, bodyDigest, routingContext, stream);
            });
        });
    }

    /** Answers {@code response} after releasing the upload's lock. */
    private Uni<Response> releasingLock(String uploadID, Response response) {
        return uploadStore.releaseLock(uploadID).replaceWith(response);
    }

    /**
     * The checks that must happen under the upload's lock — the record as it stands now, a
     * deferred length becoming known, the offset — and then the write. The caller holds the
     * lock; every path here releases it.
     */
    private Uni<Response> patchUnderLockValidated(String uploadID, long uploadOffset, Long contentLength,
                                                  Long uploadLength, ChecksumInfo checksumInfo,
                                                  MessageDigest digest, RoutingContext routingContext,
                                                  AtomicReference<ChunkStream> stream) {
        return uploadStore.findUploadInfo(uploadID).chain(lockedInfoOpt -> {
            if (lockedInfoOpt.isEmpty()) {
                return releasingLock(uploadID, tus(NOT_FOUND).build());
            }
            UploadInfo info = lockedInfoOpt.get();

            boolean lengthStillDeferred = info.isDeferredLength() && info.getEntityLength() < 0;
            Uni<Boolean> stillDeferred;
            if (uploadLength != null && lengthStillDeferred) {
                if (uploadLength < 0 || uploadLength > tusRuntimeConfig.maxSize()) {
                    return releasingLock(uploadID, tus(BAD_REQUEST).entity("Failed to set upload length").build());
                }
                info.setEntityLength(uploadLength);
                info.setDeferredLength(false);
                info.setLastActivity(Instant.now());
                stillDeferred = uploadStore.updateUploadInfo(uploadID, info)
                        .invoke(() -> {
                            events.lengthKnown(uploadID, uploadLength);
                            LOG.infof("Set deferred length for upload %s to %d", uploadID, uploadLength);
                            // The declaring PATCH itself may carry no data (data-first,
                            // declare-last): all the bytes already landed while the length was
                            // still deferred, so the offset already equals the length being
                            // declared here and nothing downstream will ever see the write that
                            // "reaches" it. Fire completion now instead. Covers uploadLength==0
                            // too, since offset is then also 0.
                            if (info.getOffset() == uploadLength) {
                                events.uploadCompleted(uploadID, info);
                            }
                        })
                        .replaceWith(false);
            } else {
                stillDeferred = Uni.createFrom().item(lengthStillDeferred);
            }

            return stillDeferred.chain(deferred -> {
                // Per the creation-defer-length extension, Upload-Length may be announced on ANY
                // PATCH, not necessarily the first one carrying data -- a client streaming a
                // source of unknown length has to send chunks while the length is still
                // deferred, and only learns (and declares) it once its source runs dry. So a
                // PATCH that leaves the length still deferred is not itself an error; the chunk
                // boundary check just below is skipped instead (it needs the entity length, and
                // there isn't one yet).

                if (uploadOffset != info.getOffset()) {
                    return releasingLock(uploadID, tus(CONFLICT)
                            .header("Upload-Offset", String.valueOf(info.getOffset()))
                            .entity("Upload offset mismatch").build());
                }

                if (!deferred && contentLength != null
                        && !fitsWithin(contentLength, uploadOffset, info.getEntityLength())) {
                    return releasingLock(uploadID,
                            tus(CONFLICT).entity("Chunk exceeds declared upload size").build());
                }

                // While the length stays deferred, fitsWithin() above is skipped (there is no
                // entity length yet to check against) -- so nothing else stops a client from
                // PATCHing data forever without ever declaring it. Enforce the server-wide cap
                // directly against the running offset instead, the same limit and same 413 the
                // declared-length paths already use.
                if (deferred && contentLength != null
                        && !fitsWithin(contentLength, uploadOffset, tusRuntimeConfig.maxSize())) {
                    return releasingLock(uploadID,
                            tus(REQUEST_ENTITY_TOO_LARGE).entity("Upload exceeds maximum allowed size").build());
                }

                return patchUnderLock(uploadID, info, uploadOffset, routingContext,
                        checksumInfo, digest, contentLength, stream);
            });
        });
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

        // Some clients poll with an empty PATCH; the store never sees a chunk it could not write.
        Uni<Long> write = contentLength != null && contentLength == 0
                ? writer.writeNothing(uploadID, uploadOffset, entityLength)
                : writer.write(uploadID, info, uploadOffset, routingContext, checksumInfo, digest,
                        contentLength, stream);

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

    /** Maps the failures {@link UploadWriter} can produce to their TUS responses. */
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
                    return switch (limit.kind()) {
                        case CHUNK_SIZE ->
                            tus(REQUEST_ENTITY_TOO_LARGE).entity("Chunk size exceeds maximum allowed size").build();
                        case MAX_SIZE ->
                            tus(REQUEST_ENTITY_TOO_LARGE).entity("Upload exceeds maximum allowed size").build();
                        case ENTITY_LENGTH ->
                            tus(CONFLICT).entity("Chunk exceeds declared upload size").build();
                    };
                })
                .onFailure(UploadNotFoundException.class).recoverWithItem(e -> tus(NOT_FOUND).build())
                .onFailure().recoverWithItem(e -> {
                    LOG.error("Error while writing to upload " + uploadID, e);
                    return tus(INTERNAL_SERVER_ERROR).entity("Internal server error").build();
                });
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
    public Uni<Response> delete(
            @PathParam("uploadID") String uploadID,
            @HeaderParam("Tus-Resumable") String tusResumable,
            @Context SecurityContext securityContext
    ) {
        if (tusResumable == null || !tusResumable.equals(tusRuntimeConfig.version())) {
            return Uni.createFrom().item(Response.status(412)
                    .header("Tus-Version", tusRuntimeConfig.version())
                    .build());
        }

        if (!TusUtils.isValidUuid(uploadID)) {
            return Uni.createFrom().item(tus(BAD_REQUEST).entity("Invalid upload ID format").build());
        }

        String currentUserId = getCurrentUserId(securityContext);
        return uploadStore.findUploadInfo(uploadID).chain(infoOpt -> {
            if (infoOpt.isPresent() && isOwnershipDenied(infoOpt.get(), currentUserId)) {
                return Uni.createFrom().item(tus(NOT_FOUND).build());
            }
            // Deleting something that was never there stays idempotent, but a write holding the
            // lock means the client should retry.
            return discard(uploadID)
                    .emitOn(Infrastructure.getDefaultWorkerPool())
                    .map(outcome -> {
                        if (outcome == Discard.LOCKED) {
                            return tus(423).entity("Upload is currently being processed").build();
                        }
                        LOG.infof("UploadID %s deleted=%s", uploadID, outcome == Discard.REMOVED);
                        events.uploadTerminated(uploadID);
                        return tus(NO_CONTENT).build();
                    });
        });
    }

    // ---------- helpers ----------

    private enum Discard { REMOVED, ABSENT, LOCKED }

    /**
     * Discards an upload the way every path in here must: under its lock, so nothing is deleted
     * underneath an in-flight write, and with the framework's own progress bookkeeping cleared.
     */
    private Uni<Discard> discard(String uploadId) {
        return uploadStore.acquireLock(uploadId).chain(locked -> {
            if (!locked) {
                return Uni.createFrom().item(Discard.LOCKED);
            }
            return uploadStore.discardUpload(uploadId)
                    .eventually(() -> uploadStore.releaseLock(uploadId))
                    .invoke(() -> events.uploadDiscarded(uploadId))
                    .map(removed -> removed ? Discard.REMOVED : Discard.ABSENT);
        });
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

    private boolean isOwnershipDenied(UploadInfo info, String currentUserId) {
        return authorizer.isDenied(info, currentUserId);
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
