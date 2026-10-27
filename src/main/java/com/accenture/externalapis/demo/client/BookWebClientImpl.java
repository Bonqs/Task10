package com.accenture.externalapis.demo.client;

import com.accenture.externalapis.demo.config.ExternalServiceProperties;
import com.accenture.externalapis.demo.dto.BookApiResponse;
import com.accenture.externalapis.demo.dto.BookDto;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.client.WebClientRequestException;
import org.springframework.web.reactive.function.client.WebClientResponseException;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeoutException;

@Component
public class BookWebClientImpl implements BookWebClient {

    private WebClient webClient;

    private Retry retryStrategy = Retry.fixedDelay(2, Duration.ofSeconds(2))
            .filter(ex -> (ex instanceof WebClientResponseException webex && webex.getStatusCode().is5xxServerError())
                    || ex instanceof WebClientRequestException || ex instanceof TimeoutException)
            .onRetryExhaustedThrow((spec, signal) -> signal.failure());

    public BookWebClientImpl(WebClient.Builder builder, ExternalServiceProperties properties) {
        this.webClient = builder.baseUrl(properties.baseUrl()).build();
    }

    @Override
    public Mono<BookDto> getBookAsync(Long id) {
        return webClient.get().uri("/books/{id}", id).retrieve()
                .bodyToMono(BookApiResponse.class).map(this::toDto)
                .switchIfEmpty(Mono.error(new ClientException("Empty response fetching book " + id)))
                .timeout(Duration.ofSeconds(3))
                .retryWhen(retryStrategy)
                .onErrorMap(WebClientResponseException.class,
                        ex -> new ClientException("HTTP error fetching book " + id + ": " + ex.getStatusCode(), ex))
                .onErrorMap(WebClientRequestException.class,
                        ex -> new ClientException("Connection refused or timeout fetching book " + id, ex))
                .onErrorMap(TimeoutException.class,
                        ex -> new ClientException("Timeout fetching book " + id, ex))
                .onErrorMap(ex -> !(ex instanceof ClientException),
                        ex -> new ClientException("Unexpected error fetching book " + id, ex));
    }

    @Override
    public Flux<BookDto> getAllBooksAsync() {
        return webClient.get().uri("/books").retrieve()
                .bodyToFlux(BookApiResponse.class)
                .map(this::toDto)
                .timeout(Duration.ofSeconds(5))
                .retryWhen(retryStrategy)
                .onErrorMap(WebClientResponseException.class,
                        ex -> new ClientException("HTTP error fetching books " + ex.getStatusCode(), ex))
                .onErrorMap(WebClientRequestException.class,
                        ex -> new ClientException("Connection refused or timeout fetching books ", ex))
                .onErrorMap(TimeoutException.class,
                        ex -> new ClientException("Timeout fetching books ", ex))
                .onErrorMap(ex -> !(ex instanceof ClientException),
                        ex -> new ClientException("Unexpected error fetching books ", ex));
    }

    @Override
    public Mono<List<BookDto>> getBooksInParallel(Long id1, Long id2) {
        Mono<BookDto> book1 = getBookAsync(id1);
        Mono<BookDto> book2 = getBookAsync(id2);
        return Mono.zip(book1, book2).map(tuple -> List.of(tuple.getT1(), tuple.getT2()));
    }

    private BookDto toDto(BookApiResponse response) {
        return new BookDto(response.title(), response.author(), response.genre(), response.price());
    }
}
