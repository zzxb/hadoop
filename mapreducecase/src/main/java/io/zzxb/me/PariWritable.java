package io.zzxb.me;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by zzxb on 17/7/27.
 */
public class PariWritable implements WritableComparable<PariWritable> {
    private String stuId;
    private String stuName;

    public String getStuId() {
        return stuId;
    }

    public void setStuId(String stuId) {
        this.stuId = stuId;
    }

    public String getStuName() {
        return stuName;
    }

    public void setStuName(String stuName) {
        stuName = stuName;
    }

    public PariWritable() {
    }

    public PariWritable(String stuId, String stuName) {
        this.stuId = stuId;
        stuName = stuName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof PariWritable)) return false;

        PariWritable that = (PariWritable) o;

        if (!getStuId().equals(that.getStuId())) return false;
        return getStuName().equals(that.getStuName());
    }

    @Override
    public int hashCode() {
        int result = getStuId().hashCode();
        result = 31 * result + getStuName().hashCode();
        return result;
    }

    @Override
    public String toString() {
        return stuId + '\t' +
                stuName;
    }

    public int compareTo(PariWritable o) {
        int comp = this.stuId.compareTo(o.getStuId());
        if(comp != 0){
            return comp;
        }
        return this.getStuName().compareTo(o.getStuName());
    }

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(this.getStuId());
        dataOutput.writeUTF(this.getStuName());
    }

    public void readFields(DataInput dataInput) throws IOException {
        this.stuId = dataInput.readUTF();
        this.stuName = dataInput.readUTF();
    }
}
