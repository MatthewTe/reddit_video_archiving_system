package com.reddit.label.Parsers.MPDUtils;

public class DashPeriod {
    int periodId;
    String videoUrl;
    String audioUrl;

    public DashPeriod() {
    }

    public int getPeriodId() {
        return periodId;
    }

    public void setPeriodId(int periodId) {
        this.periodId = periodId;
    }

    public String getVideoUrl() {
        return videoUrl;
    }

    public void setVideoUrl(String videoUrl) {
        this.videoUrl = videoUrl;
    }

    public String getAudioUrl() {
        return audioUrl;
    }

    public void setAudioUrl(String audioUrl) {
        this.audioUrl = audioUrl;
    }

    @Override
    public String toString() {
        return "DashPeriod [periodId=" + periodId + ", videoUrl=" + videoUrl + ", audioUrl=" + audioUrl + "]";
    }


}
