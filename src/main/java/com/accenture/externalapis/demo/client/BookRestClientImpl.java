package com.accenture.externalapis.demo.client;

import com.accenture.externalapis.demo.config.ExternalServiceProperties;
import com.accenture.externalapis.demo.dto.BookApiResponse;
import com.accenture.externalapis.demo.dto.BookDto;
import org.springframework.stereotype.Component;
import org.springframework.web.client.*;

import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;

@Component
public class BookRestClientImpl implements BookRestClient {

    private RestClient restClient;

    public BookRestClientImpl(RestClient.Builder builder, ExternalServiceProperties properties) {
        this.restClient = builder.baseUrl(properties.baseUrl()).build();
    }

    @Override
    public BookDto getBook(Long id) {
        BookApiResponse response = execute("fetching book " + id,
                () -> restClient.get().uri("/books/{id}", id).retrieve().body(BookApiResponse.class));
        return toDto(response);
    }

    @Override
    public List<BookDto> getAllBooks() {
        BookApiResponse[] responses = execute("fetching all books",
                () -> restClient.get().uri("/books").retrieve().body(BookApiResponse[].class));
        return Arrays.stream(responses).map(this::toDto).toList();
    }

    private BookDto toDto(BookApiResponse response) {
        return new BookDto(response.title(), response.author(), response.genre(), response.price());
    }

    private <T> T execute(String context, Supplier<T> request) {
        try {
            T result = request.get();
            if (result == null) {
                throw new ClientException("Empty response " + context);
            }
            return result;
        } catch (HttpClientErrorException ex) {
            throw new ClientException("Client error " + context + ": " + ex.getStatusCode(), ex);
        } catch (HttpServerErrorException ex) {
            throw new ClientException("Server error " + context + ": " + ex.getStatusCode(), ex);
        } catch (ResourceAccessException ex) {
            throw new ClientException("Connection refused or timeout " + context, ex);
        } catch (RestClientException ex) {
            throw new ClientException("Unexpected error " + context, ex);
        }
    }
}
