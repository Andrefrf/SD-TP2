package sys.storage;

import api.storage.Datanode;
import com.github.scribejava.core.builder.ServiceBuilder;
import com.github.scribejava.core.model.OAuth2AccessToken;
import com.github.scribejava.core.model.OAuthRequest;
import com.github.scribejava.core.model.Response;
import com.github.scribejava.core.model.Verb;
import com.github.scribejava.core.oauth.OAuth20Service;
import org.pac4j.scribe.builder.api.DropboxApi20;
import sys.storage.DropDeleteArgs;
import sys.storage.DownloadDropArgs;
import sys.storage.UploadDropArgs;
import utils.JSON;
import utils.Random;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response.Status;
import java.io.IOException;
import java.util.concurrent.ExecutionException;

public class DatanodeDropboxResources implements Datanode {

    private static final int OK = 200;
    private static final int CONFLICT = 409;

    private static final String apiKey = "3alfbzzm7k7igae";
    private static final String apiSecret = "fo890efedbyi2s3";

    private static final String DROPBOX_UPLOAD_FILE = "https://content.dropboxapi.com/2/files/upload";
    private static final String DROPBOX_DELETE_FILE = "https://api.dropboxapi.com/2/files/delete_v2";
    private static final String DROPBOX_DOWNLOAD_FILE = "https://content.dropboxapi.com/2/files/download";
    private static final String CONTENT_TYPE = "Content-Type";
    private static final String DROPBOX_API_ARG = "Dropbox-API-Arg";
    private static final String APPLICATION_OCTET_STREAM = "application/octet-stream";
    private static final String APPLICATION_JSON = "application/json";
    private static final String PATH = "/Datanode/";

    private final String address;
    private OAuth2AccessToken accessToken;
    private OAuth20Service service;

    DatanodeDropboxResources(String oauthToken, String address) {
        this.accessToken = new OAuth2AccessToken(oauthToken);
        this.service = new ServiceBuilder()
                .apiKey(apiKey)
                .apiSecret(apiSecret)
                .build(DropboxApi20.INSTANCE);
        this.address = address + "/datanode/";
    }

    @Override
    public synchronized String createBlock(byte[] data) {
        String ID = Random.key64()/* + FILE_EXT*/;

        OAuthRequest createBlock = new OAuthRequest(Verb.POST, DROPBOX_UPLOAD_FILE);
        createBlock.addHeader(CONTENT_TYPE, APPLICATION_OCTET_STREAM);
        String s = JSON.encode(new UploadDropArgs(PATH + ID));
        System.out.println("Encoded argument: " +s);
        createBlock.addHeader(DROPBOX_API_ARG, s);
        createBlock.setPayload(data);

        service.signRequest(accessToken, createBlock);
        try {
            Response r = service.execute(createBlock);
            System.out.println("Response:" + r);

            if (r.getCode() == CONFLICT) {
                throw new WebApplicationException(Status.NOT_FOUND);
            }

            if (r.getCode() != OK) {
                throw new WebApplicationException(r.getCode());
            }


            System.err.println(address + ID);
            return address + ID;
        } catch (InterruptedException | ExecutionException | IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public synchronized void deleteBlock(String block) {
        OAuthRequest deleteBlock = new OAuthRequest(Verb.POST, DROPBOX_DELETE_FILE);
        deleteBlock.addHeader(CONTENT_TYPE, APPLICATION_JSON);
        deleteBlock.setPayload(JSON.encode(new DropDeleteArgs(PATH + block)));

        service.signRequest(accessToken, deleteBlock);
        try {
            Response r = service.execute(deleteBlock);
            System.out.println("Response:" + r);

            if (r.getCode() == CONFLICT) {
                throw new WebApplicationException(Status.NOT_FOUND);
            }

            if (r.getCode() != OK) {
                throw new WebApplicationException(r.getCode());
            }
        } catch (InterruptedException | ExecutionException | IOException e) {
            System.out.println(block);
            e.printStackTrace();
        }
    }

    @Override
    public synchronized byte[] readBlock(String block) {
        OAuthRequest readBlock = new OAuthRequest(Verb.GET, DROPBOX_DOWNLOAD_FILE);
        readBlock.addHeader(DROPBOX_API_ARG, JSON.encode(new DownloadDropArgs(PATH + block)));

        service.signRequest(accessToken, readBlock);
        try {
            Response r = service.execute(readBlock);
            System.out.println("Response:" + r);

            if (r.getCode() == CONFLICT) {
                throw new WebApplicationException(Status.NOT_FOUND);
            }

            if (r.getCode() != OK) {
                throw new WebApplicationException(r.getCode());
            }

            return r.getBody().getBytes();
        } catch (InterruptedException | ExecutionException | IOException e) {
            e.printStackTrace();
        }
        return null;
    }

}
