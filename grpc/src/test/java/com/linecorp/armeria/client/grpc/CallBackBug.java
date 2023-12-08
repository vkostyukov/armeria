package com.linecorp.armeria.client.grpc;

import com.google.protobuf.ByteString;
import com.linecorp.armeria.common.grpc.GrpcSerializationFormats;
import com.linecorp.armeria.grpc.testing.Messages;
import com.linecorp.armeria.grpc.testing.UnitTestServiceGrpc;
import com.linecorp.armeria.server.ServerBuilder;
import com.linecorp.armeria.server.grpc.GrpcService;
import com.linecorp.armeria.testing.junit5.server.ServerExtension;
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.net.URI;
import java.util.concurrent.CompletableFuture;

public class CallBackBug {

    private static CompletableFuture<Void> aborted = new CompletableFuture<>();
    private static CompletableFuture<Void> cancelled = new CompletableFuture<>();
    private static CompletableFuture<Void> firstRequestHandled = new CompletableFuture<>();

    private static StreamObserver<Messages.SimpleResponse> clientObserver= new StreamObserver<Messages.SimpleResponse>() {
        @Override
        public void onNext(Messages.SimpleResponse value) {
            firstRequestHandled.complete(null);
        }

        @Override
        public void onError(Throwable t) {
            //aborted.complete(null);
        }

        @Override
        public void onCompleted() {
            //aborted.complete(null);
        }
    };

    private static final UnitTestServiceGrpc.UnitTestServiceImplBase unitTestServiceImpl = new UnitTestServiceGrpc.UnitTestServiceImplBase() {
        @Override
        public StreamObserver<Messages.SimpleRequest> streamThrowsError(StreamObserver<Messages.SimpleResponse> responseObserver) {
            ServerCallStreamObserver<Messages.SimpleResponse> serverResponseObserver =
                    (ServerCallStreamObserver<Messages.SimpleResponse>) responseObserver;

            serverResponseObserver.setOnCancelHandler(new Runnable() {
                @Override
                public void run() {
                    cancelled.complete(null);
                }
            });

            return new StreamObserver<Messages.SimpleRequest>() {
                @Override
                public void onNext(Messages.SimpleRequest value) {
                    responseObserver.onNext(
                            Messages.SimpleResponse.newBuilder()
                            .setPayload(value.getPayload())
                            .build()
                    );
                }

                @Override
                public void onError(Throwable t) {
                    aborted.complete(null);
                }

                @Override
                public void onCompleted() {
                    aborted.complete(null);
                }
            };
        }
    };

    @RegisterExtension
    public static final ServerExtension server = new ServerExtension() {
        @Override
        protected void configure(ServerBuilder sb) {
            sb.workerGroup(1);
            sb.requestTimeoutMillis(60 * 1000);
            sb.maxRequestLength(10_000_000); // 10mb
            sb.idleTimeoutMillis(0);
            sb.http(0);
            sb.https(0);
            sb.tlsSelfSigned();


            sb.serviceUnder("/",
                    GrpcService.builder()
                            .addService(unitTestServiceImpl)
                            .useClientTimeoutHeader(false)
                            .build()
            );
        }
    };

    @Test
    void onCancelCallback() throws Exception {
        final URI uri = server.httpUri(GrpcSerializationFormats.PROTO);

        UnitTestServiceGrpc.UnitTestServiceStub unitTestAsyncStub =
                GrpcClients
                        .builder(uri.getScheme(), server.httpEndpoint())
                        .responseTimeoutMillis(60 * 1000)
                        .build(UnitTestServiceGrpc.UnitTestServiceStub.class);

        ClientCallStreamObserver<Messages.SimpleRequest> serverStream = (ClientCallStreamObserver<Messages.SimpleRequest>)
                unitTestAsyncStub.streamThrowsError(clientObserver);

        final Messages.SimpleRequest request =
                Messages.SimpleRequest.newBuilder()
                        .setPayload(Messages.Payload.newBuilder()
                                .setBody(ByteString.copyFrom(new byte[10240])))
                        .build();

        serverStream.onNext(request);
        firstRequestHandled.get(); // wait for the first exchange to happen

        // Neither onError nor cancel works in 1.16. Both seems to do the right thing on main.
        //serverStream.onError(new Throwable("boom"));
        serverStream.cancel("Cancel", new Throwable("boom"));

        cancelled.get();
    }
}
