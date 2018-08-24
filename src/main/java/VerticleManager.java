import io.vertx.core.AbstractVerticle;
import io.vertx.core.CompositeFuture;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Future;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import baseVerticle.AbstractCustomVerticle;

public class VerticleManager extends AbstractCustomVerticle {

	private static final String kVerticleBase = "baseVerticle";
	private static final String kVerticleManagerFile = "verticleManager";

	private Logger logger = LoggerFactory.getLogger( VerticleManager.class.getName() );
	private HashMap<String, Object> deployMap = new HashMap<>();
    private static String serverId;

	class VerticleInfo {
		VerticleInfo( String name, int instanceCount ) {
			this.name = name;
			this.instanceCount = instanceCount;
		}

		String name;
		int instanceCount;


		//test
	}

	@Override
	public void start( Future<Void> startFuture ) throws Exception {

		logger.info( "start verticle manager id: {}, config: {}" , deploymentID(), config() );
		List<Future> futureList = makeFutureList();
		CompositeFuture.all( futureList).setHandler( handler -> {
			if( handler.succeeded() ) {
				logger.info( "completed all verticle deployed" );

			}
		});
	}

	private void handleManager( Message<Object> message ) {
		try {
			JsonObject request = ( JsonObject )message.body();
			String mode = request.getString("type");
		} catch (Exception e) {
			// TODO: handle exception
		}
	}

	private List<Future> makeFutureList() throws Exception {
		List<Future> futureList = new ArrayList<>();
		List<VerticleInfo> verticleInfoList = loadRunVerticleFiles();
		for( VerticleInfo verticleInfo : verticleInfoList ) {
			JsonObject config = config().copy().put("instance", verticleInfo.instanceCount);
			DeploymentOptions deploymentOptions = new DeploymentOptions();
			deploymentOptions.setInstances( verticleInfo.instanceCount );
			deploymentOptions.setConfig( config );
			futureList.add( deployWithFuture( verticleInfo.name , deploymentOptions, deployMap));
		}

		return futureList;
	}

	@Override
	public void stop( Future<Void> stopFuture ) throws Exception {
		logger.info( "stop vertx" + deploymentID() );
		stopFuture.complete();
	}

	private List<VerticleInfo> loadRunVerticleFiles() throws Exception {
		String splitKey = ",";
		int namePos = 0;
		int instancePos = 1;
		int maxPos = 2;

		List<VerticleInfo> verticleList = new ArrayList<>();
		try( BufferedReader bufferedReader = new BufferedReader( new  InputStreamReader( getClass().getClassLoader().getResourceAsStream( kVerticleManagerFile)))) {
			String line;
			while( (line = bufferedReader.readLine()) != null ) {

                if( isBlank( line ) ) {
                    continue;
                }

				String[] splitData = line.split( splitKey );

                if( splitData.length != maxPos ) {
                    logger.warn( "invalidate verticle data in verticleManager file" );
                    continue;
                }

	            String name = kVerticleBase + "." + splitData[ namePos ];
	            Integer instance = Integer.valueOf( splitData[ instancePos ] );

                verticleList.add( new VerticleInfo( name, instance ) );
			}
		}

		return verticleList;
	}

    private boolean isBlank( String line ) {
        return line.length() == 0;
    }



}
