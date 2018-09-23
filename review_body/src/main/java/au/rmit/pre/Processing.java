package au.rmit.pre;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Processing {
    public static void main(String[] args) {
        String s = "Hi. Hi Hi Hi. Nice to meet you";
        String pattern = "[\\w]";
        Pattern r = Pattern.compile(pattern);

        Matcher m = r.matcher(s);
        if(m.find()) {
            System.out.println(m.group(0));
    }
}
}
