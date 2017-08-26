package uk.co.mruoc.dynamo;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class FakeSimpleTableExceptionTest {

    @Test
    public void shouldReturnMessage() {
        String message = "error-message";

        FakeSimpleTableException exception = new FakeSimpleTableException(message);

        assertThat(exception.getMessage()).isEqualTo(message);
    }

}
