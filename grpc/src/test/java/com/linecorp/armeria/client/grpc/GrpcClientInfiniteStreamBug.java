package com.linecorp.armeria.client.grpc;

import com.google.protobuf.ByteString;
import com.linecorp.armeria.common.grpc.GrpcSerializationFormats;
import com.linecorp.armeria.server.ServerBuilder;
import com.linecorp.armeria.server.grpc.GrpcService;
import com.linecorp.armeria.testing.junit5.server.ServerExtension;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import testing.grpc.Messages;
import testing.grpc.UnitTestServiceGrpc;

import java.net.URI;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;


public class GrpcClientInfiniteStreamBug {

    private static CompletableFuture<Void> aborted = new CompletableFuture<>();

    private static final UnitTestServiceGrpc.UnitTestServiceImplBase unitTestServiceImpl = new UnitTestServiceGrpc.UnitTestServiceImplBase() {
        @Override
        public StreamObserver<Messages.SimpleRequest> errorFromClient(StreamObserver<Messages.SimpleResponse> responseObserver) {
            return new StreamObserver<Messages.SimpleRequest>() {
                @Override
                public void onNext(Messages.SimpleRequest value) {
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
    void infiniteStreamBug() {
        final URI uri = server.httpUri(GrpcSerializationFormats.PROTO);

        UnitTestServiceGrpc.UnitTestServiceStub unitTestAsyncStub =
                GrpcClients.builder(uri.getScheme(), server.httpEndpoint())
                .build(UnitTestServiceGrpc.UnitTestServiceStub.class);

        StreamObserver<Messages.SimpleRequest> serverStream = unitTestAsyncStub.errorFromClient(new StreamObserver<Messages.SimpleResponse>() {
            @Override
            public void onNext(Messages.SimpleResponse value) {
            }

            @Override
            public void onError(Throwable t) {
                aborted.complete(null);
            }

            @Override
            public void onCompleted() {
                aborted.complete(null);
            }
        });

        final Messages.SimpleRequest request =
                Messages.SimpleRequest.newBuilder()
                        .setPayload(Messages.Payload.newBuilder()
                                .setBody(ByteString.copyFrom(new byte[10240])))
                        .build();

        // Each payload is 10kb; it should take us 1000 iterations to reach the limit
        // so the test should terminate eventually, but it never happens as the stream never aborted.
        while (!aborted.isDone()) {
            serverStream.onNext(request);
            System.err.println("Sending a request");
        }

        assertThat(aborted.isDone()).isTrue();
    }
}
