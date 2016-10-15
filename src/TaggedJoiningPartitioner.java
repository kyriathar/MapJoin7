import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Partitioner;

	public class TaggedJoiningPartitioner extends Partitioner<TaggedKey,Text> {

	    @Override
	    public int getPartition(TaggedKey TaggedKey, Text text, int numPartitions) {
		//System.out.println("IM THEEE PARTIIINIONERRRRR");
		//return TaggedKey.getJoinKey().hashCode() % numPartitions; // negative. illegal partition
		return (TaggedKey.getJoinKey().hashCode() & Integer.MAX_VALUE) % numPartitions;
	    }
	}
