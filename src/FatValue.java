import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by hduser on 16/3/2016.
 */
public class FatValue extends Text implements Writable  {
    //private String data = new String();
    private int joinOrder;  //sto reducer wste sto joining na ksexwrisei tis eggrafes tou file1 apo tou file2

    public void set(String data ,int joinOrder){
        //this.data = data ;
        super.set(data);
        this.joinOrder = joinOrder ;
    }

    public int getJoinOrder(){
        return joinOrder ;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        super.readFields(in);
        joinOrder = in.readInt();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);
        out.writeInt(joinOrder);
    }
}
