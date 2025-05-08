import java.util.*;
import java.util.stream.Collectors;

public class CollectionFramework {

    public static void main (String args[]){

        Integer[] array= new Integer[]{1,2,1, 3,4,5,3,2,5,3};

       List<Integer> arr= Arrays.asList(array) ;
       HashSet set = new HashSet(Arrays.asList(array)) ;

        List<Integer> arr1 =Arrays.stream(array).distinct().collect(Collectors.toList());


       System.out.println(set);
        System.out.println(arr1);




    }

}
