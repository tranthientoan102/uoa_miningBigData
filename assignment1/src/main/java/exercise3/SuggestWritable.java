package exercise3;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class SuggestWritable implements Writable {

    public Long friendId;
    public Long mutualId;

    public SuggestWritable(Long friendId, Long mutualId){
        this.mutualId = mutualId;
        this.friendId = friendId;
    }
    public SuggestWritable(){
        this.friendId = -1L;
        this.mutualId = -1L;
    }

    public SuggestWritable(SuggestWritable ref) {
        this.friendId = ref.friendId;
        this.mutualId = ref.mutualId;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(friendId);
        out.writeLong(mutualId);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        friendId = in.readLong();
        mutualId = in.readLong();
    }

    @Override
    public String toString() {
        return " -> " + friendId + " via " + mutualId;
    }
}
