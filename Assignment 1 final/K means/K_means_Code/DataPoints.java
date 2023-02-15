package k_means_2;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class DataPoints implements Writable, WritableComparable<DataPoints> {
    private double x;
    private double y;
    private Integer id;

    public DataPoints(){
        this.id=0;
    }

    public DataPoints(String s){
        set(s);
    }

    public void set(String s){
        String[] num=s.split(",");
        if(num.length==3){
            if(!Character.isDigit(num[1].charAt(0))){
                num[1]=num[1].substring(1);
            }
            if(!Character.isDigit(num[2].charAt(num[2].length()-1))){
                num[2]=num[2].substring(0,num[2].length()-1);
            }
            this.x=Double.parseDouble(num[1]);
            this.y=Double.parseDouble(num[2]);
            this.id=Integer.parseInt(num[0]);
        }else if(num.length==2){
            if(!Character.isDigit(num[0].charAt(0))){
                num[0]=num[0].substring(1);
            }
            if(!Character.isDigit(num[1].charAt(num[1].length()-1))){
                num[1]=num[1].substring(0,num[1].length()-1);
            }
            this.x=Double.parseDouble(num[0]);
            this.y=Double.parseDouble(num[1]);
            this.id=0;
        }

    }

    public double getX() {
        return x;
    }

    public double getY() {
        return y;
    }

    public Integer getId() {
        return id;
    }

    public void setId(int id){
        this.id=id;
    }

    public void add(DataPoints o){

        this.x+=o.getX();
        this.y+=o.getY();

    }

    public void divide(int count){
        this.x/=count;
        this.y/=count;
    }

    @Override
    public int compareTo(DataPoints o) {
        if(this.getId().compareTo(o.getId())!=0){
            return this.getId().compareTo(o.getId());
        }else return 0;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(this.id);
        dataOutput.writeDouble(this.x);
        dataOutput.writeDouble(this.y);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.id=dataInput.readInt();
        this.x=dataInput.readDouble();
        this.y=dataInput.readDouble();
    }
    public String toString(){
        StringBuilder sb=new StringBuilder();
        sb.append(getId()).append(",").append(getX()).
                append(",").append(getY());
        return sb.toString();
    }
}