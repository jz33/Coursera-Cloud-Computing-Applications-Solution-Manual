package utilities; 
    
public class Tuple2<A extends Comparable<? super A>, B extends Comparable<? super B>>
        implements Comparable<Tuple2<A, B>> {

    public final A _1;
    public final B _2;
    
    public Tuple2(A _1, B _2){
        this._1 = _1;
        this._2 = _2;
    }

    @Override
    public int compareTo(Tuple2<A, B> that){
        int cmp = that == null ? 1 : (this._1).compareTo(that._1);
        return cmp == 0 ? (this._2).compareTo(that._2) : cmp;
    }

    @Override
    public int hashCode(){
        return 31 * _1.hashCode() + _2.hashCode();
    }

    @Override
    public boolean equals(Object obj){
        if (!(obj instanceof Tuple2))
            return false;
        if (this == obj)
            return true;
        return equal(_1, ((Tuple2<?, ?>) obj)._1) && equal(_2, ((Tuple2<?, ?>) obj)._2);
    }
    
    private boolean equal(Object o1, Object o2){
        return o1 == o2 || (o1 != null && o1.equals(o2));
    }

    public String toString(String d){
        return _1.toString() + d + _2.toString();
    }
}
