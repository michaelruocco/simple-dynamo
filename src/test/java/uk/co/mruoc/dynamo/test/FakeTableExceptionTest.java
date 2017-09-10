package uk.co.mruoc.dynamo.test;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class FakeTableExceptionTest {

    @Test
    public void shouldReturnMessage() {
        String message = "error-message";

        FakeTableException exception = new FakeTableException(message);

        assertThat(exception.getMessage()).isEqualTo(message);
    }

}
