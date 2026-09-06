package org.sitenetsoft.quarkus.tus.it;

import io.quarkus.arc.Unremovable;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Singleton;
import org.sitenetsoft.quarkus.tus.runtime.event.*;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

@Singleton
@Unremovable
public class TusTestObserver {

    public final CopyOnWriteArrayList<TusUploadCreatedEvent> createdEvents = new CopyOnWriteArrayList<>();
    public final CopyOnWriteArrayList<TusChunkReceivedEvent> chunkEvents = new CopyOnWriteArrayList<>();
    public final CopyOnWriteArrayList<TusUploadCompletedEvent> completedEvents = new CopyOnWriteArrayList<>();
    public final CopyOnWriteArrayList<TusUploadTerminatedEvent> terminatedEvents = new CopyOnWriteArrayList<>();
    public final CopyOnWriteArrayList<TusConcatenationCompletedEvent> concatEvents = new CopyOnWriteArrayList<>();

    /** Uploads whose completion event this observer throws on, the way a broken application observer might. */
    public final Set<String> failCompletionFor = ConcurrentHashMap.newKeySet();
    /** The thread each upload's completion event was delivered on. */
    public final Map<String, String> completionThreads = new ConcurrentHashMap<>();

    public void onCreated(@Observes TusUploadCreatedEvent event) {
        createdEvents.add(event);
    }

    public void onChunk(@Observes TusChunkReceivedEvent event) {
        chunkEvents.add(event);
    }

    public void onCompleted(@Observes TusUploadCompletedEvent event) {
        completedEvents.add(event);
        completionThreads.put(event.uploadId(), Thread.currentThread().getName());
        if (failCompletionFor.contains(event.uploadId())) {
            throw new IllegalStateException("observer failed on purpose for " + event.uploadId());
        }
    }

    public void onTerminated(@Observes TusUploadTerminatedEvent event) {
        terminatedEvents.add(event);
    }

    public void onConcat(@Observes TusConcatenationCompletedEvent event) {
        concatEvents.add(event);
    }

    public void reset() {
        createdEvents.clear();
        chunkEvents.clear();
        completedEvents.clear();
        terminatedEvents.clear();
        concatEvents.clear();
        failCompletionFor.clear();
        completionThreads.clear();
    }
}
