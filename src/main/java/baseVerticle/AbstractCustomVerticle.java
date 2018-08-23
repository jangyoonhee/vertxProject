package baseVerticle;

import java.util.HashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.eventbus.Message;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.impl.VertxInternal;
import io.vertx.core.json.JsonObject;
import io.vertx.spi.cluster.hazelcast.HazelcastClusterManager;

public class AbstractCustomVerticle extends AbstractVerticle{

	private static final String kSeparateValue = ",";
	private static Logger logger = LoggerFactory.getLogger( AbstractVerticle.class.getName());


    public static String getSeparateValue() {
        return kSeparateValue;
    }

	protected void deploy( String name, DeploymentOptions options ) {

		vertx.deployVerticle( name, options, completeHaneler ->{
			if( completeHaneler.succeeded() ) {
				logger.info( "completed verticleed deployed: name: {}, deploymentId:{} " , completeHaneler.result() );
			}else {
				logger.error( "fail deploy verticle - { }", completeHaneler.cause().getMessage() );
			}

		});
	}

	protected Future<String> deployWithFuture( String name, DeploymentOptions options ) {
		Future<String> future = Future.future();
		vertx.deployVerticle( name, options, completeHandler ->{
			if( completeHandler.succeeded() ) {
				logger.info( "completed verticle deployed - name: {}, deploymentId : {}", name, completeHandler.result() );
				future.complete( completeHandler.result() );
			} else {
				logger.error( "failed deploy verticle - {}", completeHandler.cause().getMessage() );
				future.fail( completeHandler.cause() );
			}
		});
		return future;
	}


	protected Future<String> deployWithFuture( String name, DeploymentOptions options, HashMap<String, Object> deployMap ) {
		Future<String> future = Future.future();
		vertx.deployVerticle( name, options, completeHandler ->{
			if( completeHandler.succeeded() ) {
				logger.info( "completed verticle deployed - name: {}, deploymentId : {}", name, completeHandler.result() );
				JsonObject value = new JsonObject();
				value.put( "name" , name );
				value.put( "instance", options.getInstances() );
				deployMap.put( completeHandler.result(), value );
				future.complete( completeHandler.result() );
			}else {
				logger.error( "failed deploy verticle - {}", completeHandler.cause().getMessage() );
				future.fail( completeHandler.cause() );
			}
		});
		return future;
	}


    protected void undeploy( String deploymentId ) {
        vertx.undeploy( deploymentId, completeHandler -> {
            if( completeHandler.succeeded() ) {
                logger.info( "completed undeployed - deploymentId: {}", deploymentId );
            } else {
                logger.error( "failed undeploy verticle - {}", completeHandler.cause().getMessage() );
            }
        } );
    }

    protected void undeploy( String deploymentId, HashMap<String, Object> deployMap ) {
        vertx.undeploy( deploymentId, completeHandler -> {
            if( completeHandler.succeeded() ) {
                logger.info( "completed undeployed - deploymentId: {}", deploymentId );
                deployMap.remove( deploymentId );
            } else {
                logger.error( "failed undeploy verticle - {}", completeHandler.cause().getMessage() );
            }
        } );
    }

    protected Future<Void> undeployWithFuture( String deploymentId ) {
        Future<Void> future = Future.future();
        vertx.undeploy( deploymentId, completeHandler -> {
            if( completeHandler.succeeded() ) {
                logger.info( "completed undeployed - deploymentId: {}", deploymentId );
                future.complete();
            } else {
                logger.error( "failed undeploy verticle - {}", completeHandler.cause().getMessage() );
                future.fail( completeHandler.cause() );
            }
        } );
        return future;
    }

    protected Future<Void> undeployWithFuture( String deploymentId, HashMap<String, Object> deployMap ) {
        Future<Void> future = Future.future();
        vertx.undeploy( deploymentId, completeHandler -> {
            if( completeHandler.succeeded() ) {
                logger.info( "completed undeployed - deploymentId: {}", deploymentId );
                deployMap.remove( deploymentId );
                future.complete();
            } else {
                logger.error( "failed undeploy verticle - {}", completeHandler.cause().getMessage() );
                future.fail( completeHandler.cause() );
            }
        } );
        return future;
    }

    protected Future<MessageConsumer<Object>> addEventBus( String address, Handler<Message<Object>> handler ) {
    	Future<MessageConsumer<Object>> future = Future.future();
    	MessageConsumer<Object> consumer = vertx.eventBus().consumer(address);
    	consumer.handler( handler );
    	consumer.exceptionHandler( exceptionHandler -> {
    		logger.error( "event bus exception - address: {}, deploymentId: {}, message: {}", address, deploymentID(), exceptionHandler.getMessage() );
    	} );
    	consumer.endHandler( endHandler -> {
    		logger.info( "event bus end handler - address: {}, deploymentId: {}", address, deploymentID() );
    	} );
    	consumer.completionHandler( completeHandler -> {
    		if ( completeHandler.succeeded() ) {
    			logger.info( "complete add event bus - address: {}, deploymentId: {}", address, deploymentID() );
    			future.complete( consumer );
    		} else {
    			logger.error( "failed add event bus - address: {}, deploymentId: {}", address, deploymentID() );
    			future.fail( completeHandler.cause() );
    		}
    	} );
    	return future;
    }


    protected HazelcastClusterManager getClusterManager() {

    	if( !vertx.isClustered() ) {
    		return null;
    	}

    	HazelcastClusterManager clusterManager = ( HazelcastClusterManager ) ( (VertxInternal) vertx ).getClusterManager();
    	return clusterManager;
    }
}
