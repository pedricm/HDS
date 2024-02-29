package pt.ulisboa.tecnico.hdsledger.utilities;

import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;

public class ObjToString {

    public static String objToString(Object object) {
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(baos);
            oos.writeObject(object);
            oos.close();
            return baos.toString();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }
}