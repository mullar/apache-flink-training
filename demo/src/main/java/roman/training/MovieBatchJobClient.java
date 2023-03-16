package roman.training;

import java.io.File;
import java.util.concurrent.TimeUnit;

import com.nextbreakpoint.flinkclient.api.ApiException;
import com.nextbreakpoint.flinkclient.api.FlinkApi;
import com.nextbreakpoint.flinkclient.model.JarRunResponseBody;
import com.nextbreakpoint.flinkclient.model.JarUploadResponseBody;
import com.nextbreakpoint.flinkclient.model.JobExecutionResultResponseBody;
import com.nextbreakpoint.flinkclient.model.QueueStatus;

public class MovieBatchJobClient {    
    public static void main(String[] args) throws Throwable {
        FlinkApi api = new FlinkApi();
        api.getApiClient().setBasePath("http://172.19.0.3:8081");
        api.getApiClient().getHttpClient().setConnectTimeout(60*1000, TimeUnit.MILLISECONDS);
        api.getApiClient().getHttpClient().setWriteTimeout(60*1000, TimeUnit.MILLISECONDS);
        api.getApiClient().getHttpClient().setReadTimeout(60*1000, TimeUnit.MILLISECONDS);
        
        JarUploadResponseBody jarUploadResponse = api.uploadJar(new File("/workspaces/apache-flink-training/demo/target/demo-1.0-SNAPSHOT.jar"));
        String fileName = new File(jarUploadResponse.getFilename()).getName();

        JarRunResponseBody jarRunResponse = null;
       
        try {
            jarRunResponse = api.runJar(fileName, true, null, null, null, MoviesDataUploadJob.class.getName(), null);
        } catch (ApiException e) {
            System.out.println(e.getResponseBody());
            throw e.fillInStackTrace();
        }        

        JobExecutionResultResponseBody jobExecutionResultResponseBody = null;

        do {
            Thread.sleep(5 * 1000);
            jobExecutionResultResponseBody = api.getJobResult(jarRunResponse.getJobid());

        } while ( jobExecutionResultResponseBody.getStatus().getId() == QueueStatus.IdEnum.IN_PROGRESS);
    }
}
