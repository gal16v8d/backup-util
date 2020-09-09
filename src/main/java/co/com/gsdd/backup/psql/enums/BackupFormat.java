package co.com.gsdd.backup.psql.enums;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor(access = AccessLevel.PACKAGE)
public enum BackupFormat {

	CUSTOM("c", ".backup"), PLAIN("p", ".sql"), TAR("t", ".tar");

	private final String command;
	private final String extension;

}
