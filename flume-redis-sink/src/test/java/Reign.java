import java.util.*;

/**
 * Created by moyong on 17/6/28.
 */
public class Reign {


    Map<Integer,LinkedList<A>> reign = new HashMap<Integer, LinkedList<A>>();

    class A{
        public A(int min,int max){
            this.max=max;
            this.min=min;
            this.len=max-min;
        }
        int max;
        int min;
        int len;
        @Override
        public String toString() {
            return "A [max=" + max + ", min=" + min + ", len=" + len + "]";
        }
    }

    public void init(List<A> aList){
        for(A a:aList){
            for(int i=a.min;i<=a.max;i++){
                Integer index = Integer.valueOf(i);
                LinkedList<A> ll = reign.get(index);
                if(ll==null){
                    ll= new LinkedList<A>();
                    ll.add(a);
                }else{
                    //遍历list，保证list按照len从小到达排序
                    for(int j=0;j<ll.size();j++){
                        A aa = ll.get(j);
                        if(a.len<=aa.len){
                            ll.add(j, a);
                            break;
                        }else if(j==ll.size()-1){
                            ll.addLast(a);
                        }
                    }
                }
                reign.put(index, ll);
            }
        }
    }

    public static void main(String[] args) {
        Reign r = new Reign();
        List<A> a = new ArrayList<A>();
        a.add(r.new A(1, 100));
        a.add(r.new A(20, 30));
        a.add(r.new A(11, 30));
        a.add(r.new A(50, 150));
        a.add(r.new A(1, 1000));
        a.add(r.new A(200, 500));
        a.add(r.new A(300, 350));
        a.add(r.new A(500, 1000));
        a.add(r.new A(900, 1000));
        r.init(a);

        long s=System.currentTimeMillis();

        System.out.println(r.reign.get(Integer.valueOf(149)).get(0));

        long e=System.currentTimeMillis();
        System.out.println(e-s);

    }


}
