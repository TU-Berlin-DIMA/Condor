package Sketches;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;

import java.util.Collection;

public class SketchingWindow extends WindowAssigner {
    /**
     * Returns a {@code Collection} of windows that should be assigned to the element.
     *
     * @param element   The element to which windows should be assigned.
     * @param timestamp The timestamp of the element.
     * @param context   The {@link WindowAssignerContext} in which the assigner operates.
     */
    @Override
    public Collection assignWindows(Object element, long timestamp, WindowAssignerContext context) {
        return null;
    }

    /**
     * Returns the default trigger associated with this {@code WindowAssigner}.
     *
     * @param env
     */
    @Override
    public Trigger getDefaultTrigger(StreamExecutionEnvironment env) {
        return null;
    }

    /**
     * Returns a {@link TypeSerializer} for serializing windows that are assigned by
     * this {@code WindowAssigner}.
     *
     * @param executionConfig
     */
    @Override
    public TypeSerializer getWindowSerializer(ExecutionConfig executionConfig) {
        return null;
    }

    /**
     * Returns {@code true} if elements are assigned to windows based on event time,
     * {@code false} otherwise.
     */
    @Override
    public boolean isEventTime() {
        return false;
    }
}
