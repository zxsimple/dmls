package com.zxsimple.dmls.common.metadata.model;

import javax.persistence.*;

@Entity
@Table(name = "dmls_config")
public class SystemConfigItem {

	@Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;
	@Column(name="system_type")
	private String systemType;
	@Column(name="config_key")
	private String configKey;
	@Column(name="config_value")
	private String configValue;

	public Long getId() {
		return id;
	}

	public void setId(Long id) {
		this.id = id;
	}

	public String getSystemType() {
		return systemType;
	}

	public void setSystemType(String systemType) {
		this.systemType = systemType;
	}

	public String getConfigKey() {
		return configKey;
	}

	public void setConfigKey(String configKey) {
		this.configKey = configKey;
	}

	public String getConfigValue() {
		return configValue;
	}

	public void setConfigValue(String configValue) {
		this.configValue = configValue;
	}

	@Override
	public String toString() {
		return "SystemConfigItem{" +
				"id=" + id +
				", systemType='" + systemType + '\'' +
				", configKey='" + configKey + '\'' +
				", configValue='" + configValue + '\'' +
				'}';
	}
}
