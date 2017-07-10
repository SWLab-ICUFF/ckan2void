package test;

public class NewClass5 {

    public static void main(String[] args) {
        String s = "fgsdf,fgasdg,"
                + "asdgfas,     ,, \ndfadsf\n"
                + ",\tdasdf";
        for (String string : s.split("[,\n\\p{Blank}]++"))
            System.out.println(string);
    }
}
