package structuredStreamingJDemo;

import java.sql.Date;

public class DeviceData {
    private String device;
    private String deviceType;
    private Double signal;
    private java.sql.Date time;

    public String getDevice() {
        return device;
    }

    public void setDevice(String device) {
        this.device = device;
    }

    public String getDeviceType() {
        return deviceType;
    }

    public void setDeviceType(String deviceType) {
        this.deviceType = deviceType;
    }

    public Double getSignal() {
        return signal;
    }

    public void setSignal(Double signal) {
        this.signal = signal;
    }

    public Date getTime() {
        return time;
    }

    public void setTime(Date time) {
        this.time = time;
    }
}
