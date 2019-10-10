package Synopsis;

public interface InvertibleSynopsis<T> extends Synopsis<T> {
    InvertibleSynopsis<T> invert(InvertibleSynopsis<T> toRemove) throws Exception;

    void decrement(T toDecrement);

    @Override
    InvertibleSynopsis<T> merge(Synopsis<T> other) throws Exception;
}
