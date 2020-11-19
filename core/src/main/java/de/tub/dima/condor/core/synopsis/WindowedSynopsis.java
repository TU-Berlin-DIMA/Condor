package de.tub.dima.condor.core.synopsis;

public class WindowedSynopsis<S extends Synopsis> implements Synopsis{
    private long windowStart;
    private long windowEnd;
    private S synopsis;

    public WindowedSynopsis(S synopsis, long windowStart, long windowEnd) {
        this.windowStart = windowStart;
        this.windowEnd = windowEnd;
        this.synopsis = synopsis;
    }

    public long getWindowStart() {
        return windowStart;
    }

    public long getWindowEnd() {
        return windowEnd;
    }

    public S getSynopsis() {
        return synopsis;
    }

    @Override
    public void update(Object element) {
        synopsis.update(element);
    }
}
