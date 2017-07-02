/**
 * Created by moyong on 17/6/28.
 */
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
/*
*
* 数据存在 redis；数据不一定采用 set score 格式 ；可以用 hash 或者 list;ip 为 key
* 取出到本地进行计算 获取到计算结果；
*
 */
public class RunTest implements Comparable<RunTest> {
    /**
     * 区间下限
     */
    private int floor;

    /**
     * 区间上限
     */
    private int ceil;

    /**
     * 区间跨度
     */
    private int span;

    public int getFloor() {
        return floor;
    }

    public void setFloor(int floor) {
        this.floor = floor;
    }

    public int getCeil() {
        return ceil;
    }

    public void setCeil(int ceil) {
        this.ceil = ceil;
    }

    public int getSpan() {
        return span;
    }

    public void setSpan(int span) {
        this.span = span;
    }

    public int compareTo(RunTest o) {//区间小的排在前面
        if(this.span>o.getSpan()){
            return 1;
        }
        else if(this.span<o.getSpan()){
            return -1;
        }
        else{
            return 0;
        }
    }

    public String toString(){
        return "["+this.getFloor()+","+this.getCeil()+"]";
    }
    public static void main(String[] args) {
        List<RunTest> regions= new ArrayList<RunTest>();

        RunTest A0= new RunTest();
        A0.setFloor(1);
        A0.setCeil(3000);
        A0.setSpan(A0.getCeil()-A0.getFloor());

        RunTest A1= new RunTest();
        A1.setFloor(10);
        A1.setCeil(20);
        A1.setSpan(A1.getCeil()-A1.getFloor());

        RunTest A2= new RunTest();
        A2.setFloor(30);
        A2.setCeil(100);
        A2.setSpan(A2.getCeil()-A2.getFloor());
        regions.add(A0);
        regions.add(A1);
        regions.add(A2);

        long s=System.currentTimeMillis();


        int test=70;
        List<RunTest> result= new ArrayList<RunTest>();
        for(RunTest region:regions){
               System.out.println(region);

            if(region.getFloor()<=test&&region.getCeil()>=test){
                result.add(region);

            }
        }
        System.out.println(result.size());

        Collections.sort(result);
        System.out.println(result.size());

        System.out.println(result.get(0));

        long e=System.currentTimeMillis();
        System.out.println(e-s);

    }
}