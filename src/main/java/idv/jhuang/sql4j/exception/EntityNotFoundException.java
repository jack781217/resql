package idv.jhuang.sql4j.exception;

@SuppressWarnings("serial")
public class EntityNotFoundException extends RuntimeException {
	public EntityNotFoundException(String format, Object... values) {
		super(String.format(format, values));
	}
}
