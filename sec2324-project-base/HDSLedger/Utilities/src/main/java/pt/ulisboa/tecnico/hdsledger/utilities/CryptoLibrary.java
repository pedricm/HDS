package pt.ulisboa.tecnico.hdsledger.utilities;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.FileNotFoundException;
import java.security.KeyFactory;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.Signature;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.PKCS8EncodedKeySpec;
import java.security.spec.X509EncodedKeySpec;
import java.security.InvalidKeyException;
import java.security.SignatureException;
import java.util.Base64;

public class CryptoLibrary {
    ///////////////////////////////////////////////////////////////////////// General-func ///////////////////////////////////////////////////////////////////////////////////
    // PRIV/PUB KEYS READ
    public static PublicKey readPublicKeyB64(String publicKeyB64) {
        try{
            byte[] pubEncoded = Base64.getDecoder().decode(publicKeyB64) ;
            X509EncodedKeySpec pubSpec = new X509EncodedKeySpec(pubEncoded);
            KeyFactory keyFactory = KeyFactory.getInstance("RSA");
            PublicKey publicK = keyFactory.generatePublic(pubSpec);
            return publicK;
        } catch (NoSuchAlgorithmException | InvalidKeySpecException e) {
            e.printStackTrace();
            return null;
        }
    }
    public static PublicKey readPublicKey(String publicKeyPath) {
        try{
            byte[] pubEncoded = readFile(publicKeyPath);
            X509EncodedKeySpec pubSpec = new X509EncodedKeySpec(pubEncoded);
            KeyFactory keyFactory = KeyFactory.getInstance("RSA");
            PublicKey publicK = keyFactory.generatePublic(pubSpec);
            return publicK;
        } catch (IOException | NoSuchAlgorithmException | InvalidKeySpecException e) {
            e.printStackTrace();
            return null;
        }
    }
    public static PrivateKey readPrivateKey(String privateKeyPath) {
        try{
            byte[] privEncoded = readFile(privateKeyPath);
            PKCS8EncodedKeySpec privSpec = new PKCS8EncodedKeySpec(privEncoded);
            KeyFactory keyFactory = KeyFactory.getInstance("RSA");
            PrivateKey priv = keyFactory.generatePrivate(privSpec);
            return priv;
        } catch (IOException | NoSuchAlgorithmException | InvalidKeySpecException e) {
            e.printStackTrace();
            return null;
        }
    }
    private static byte[] readFile(String path) throws FileNotFoundException, IOException {
        FileInputStream fis = new FileInputStream(path);
        byte[] content = new byte[fis.available()];
        fis.read(content);
        fis.close();
        return content;
    }
    public static String pubKeyToString(PublicKey pubKey){
        if(pubKey == null) return null;
        byte[] pubKeyBytes = pubKey.getEncoded();
        return Base64.getEncoder().encodeToString(pubKeyBytes);
    }
    public static PublicKey stringToPubKey(String key){
        byte[] keyBytes = Base64.getDecoder().decode(key);
        try {
            KeyFactory keyfac = KeyFactory.getInstance("RSA");
            X509EncodedKeySpec spec = new X509EncodedKeySpec(keyBytes);
            return keyfac.generatePublic(spec);
        }
        catch (NoSuchAlgorithmException | InvalidKeySpecException e) {
            e.printStackTrace();
            return null;
        }
    }
    ///////////////////////////////////////////////////////////////////////// Create-DS-Subfunc ///////////////////////////////////////////////////////////////////////////////////
    // DIGITAL SIGNATURE
    public static String digitalSignature(String obj, PrivateKey privateKey) {
        if (obj == null) return null;
        try {
            Signature sig = Signature.getInstance("SHA256withRSA");
            sig.initSign(privateKey);
            sig.update(obj.getBytes());
            byte[] signature = sig.sign();
            return signatureToBase64(signature);
        } catch (NoSuchAlgorithmException | InvalidKeyException | SignatureException e) {
            e.printStackTrace();
            return null;
        }
    }
    private static String signatureToBase64(byte[] signature) {
        return Base64.getEncoder().encodeToString(signature);
    }

    ///////////////////////////////////////////////////////////////////////// Create-DS ///////////////////////////////////////////////////////////////////////////////////
    public static String CreateDS(Object obj, String pathPrivServerKey) {
        PrivateKey privateKey = readPrivateKey(pathPrivServerKey);
        //create DS
        return digitalSignature(ObjToString.objToString(obj),privateKey);
    }

    ///////////////////////////////////////////////////////////////////////// Check-Subfunc ///////////////////////////////////////////////////////////////////////////////////
    private static boolean verifyDigitalSignature(String receivedSignature, String obj, PublicKey pubKey) {
        if (receivedSignature == null || obj == null) return false;
        try {
            byte[] DSbytes = Base64.getDecoder().decode(receivedSignature);
            // verify the signature with the public key
            Signature sig = Signature.getInstance("SHA256withRSA");
            sig.initVerify(pubKey);
            sig.update(obj.getBytes());
            return sig.verify(DSbytes);
        } catch (NoSuchAlgorithmException | InvalidKeyException | SignatureException se) {
            System.err.println("Caught exception while verifying " + se);
            return false;
        }
    }

    ///////////////////////////////////////////////////////////////////////// Check ///////////////////////////////////////////////////////////////////////////////////
    public static Boolean Check(Object obj, String DS, String publicPathKey) {
        PublicKey publicKey = readPublicKey(publicPathKey);
        return verifyDigitalSignature(DS, ObjToString.objToString(obj), publicKey);
    }
}

