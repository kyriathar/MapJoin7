import org.apache.hadoop.io.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TaggedKey implements Writable, WritableComparable<TaggedKey> {

    private String joinKey = new String();
    private int tag ;            //(joinOrder)wste ston reducer na valei tou idiou key ,auta pou einai tou file1 prin to fiel2

     @Override
    public int compareTo(TaggedKey taggedKey) {
        int compareValue = this.joinKey.compareTo(taggedKey.getJoinKey());
        if(compareValue == 0 ){
            compareValue = Integer.compare(this.tag, taggedKey.getTag());
        }
        return compareValue;
    }

    public void set (String joinKey  , int tag ) {this.joinKey = joinKey; this.tag = tag;}

    public void setJoinKey (String joinKey ){this.joinKey = joinKey;}

    public void setTag (int tag  ){this.tag = tag;}

    public String getJoinKey() {
        return joinKey;
    }

    public int getTag() {
        return tag;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        //joinKey.readFields(in);
        joinKey = WritableUtils.readString(in);
        //tag.readFields(in);
        tag = in.readInt();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        //joinKey.write(out);
        WritableUtils.writeString(out, joinKey);
        //tag.write(out);
        out.writeInt(tag);
    }
}
