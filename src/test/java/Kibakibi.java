import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Kibakibi {

    public static void main(String[] args) {

        String regex = "\\d+.\\d+";

        Pattern pattern=Pattern.compile(regex);
        Matcher matcher = pattern.matcher("21332.32 ");


        System.out.println(matcher.matches());


    }

}
