package org.sitenetsoft.quarkus.tus.it;

import io.quarkus.arc.Unremovable;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Singleton;
import org.sitenetsoft.quarkus.tus.runtime.event.*;

import java.util.concurrent.CopyOnWriteArrayList;

@Singleton
@Unremovable
public class TusTestObserver {

    public final CopyOnWriteArrayList<TusUploadCreatedEvent> createdEvents = new CopyOnWriteArrayList<>();
    public final CopyOnWriteArrayList<TusChunkReceivedEvent> chunkEvents = new CopyOnWriteArrayList<>();
    public final CopyOnWriteArrayList<TusUploadCompletedEvent> completedEvents = new CopyOnWriteArrayList<>();
    public final CopyOnWriteArrayList<TusUploadTerminatedEvent> terminatedEvents = new CopyOnWriteArrayList<>();
    public final CopyOnWriteArrayList<TusConcatenationCompletedEvent> concatEvents = new CopyOnWriteArrayList<>();

    public void onCreated(@Observes TusUploadCreatedEvent event) {
        createdEvents.add(event);
    }

    public void onChunk(@Observes TusChunkReceivedEvent event) {
        chunkEvents.add(event);
    }

    public void onCompleted(@Observes TusUploadCompletedEvent event) {
        completedEvents.add(event);
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
    }
}
