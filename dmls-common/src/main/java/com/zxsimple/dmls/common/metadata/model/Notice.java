package com.zxsimple.dmls.common.metadata.model;

import javax.persistence.*;
import java.sql.Timestamp;

/**
 * Created by zxsimple on 6-20.
 */
@Entity
@Table(name="notice")
public class Notice {
    @Id
    @GeneratedValue
    private Long id;

    @Column(name="user_id")
    private Long userId;

    @Column(name="notice_type")
    private String noticeType;

    @Column(name="message_id")
    private Long messageId;

    @Column(name="notice_content")
    private String noticeContent;

    @Column(name="is_viewed")
    private int isViewed;

    @Column(name = "update_time")
    private Timestamp updateTime;

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public Long getUserId() {
        return userId;
    }

    public void setUserId(Long userId) {
        this.userId = userId;
    }

    public String getNoticeType() {
        return noticeType;
    }

    public void setNoticeType(String noticeType) {
        this.noticeType = noticeType;
    }

    public Long getMessageId() {
        return messageId;
    }

    public void setMessageId(Long messageId) {
        this.messageId = messageId;
    }

    public String getNoticeContent() {
        return noticeContent;
    }

    public void setNoticeContent(String noticeContent) {
        this.noticeContent = noticeContent;
    }

    public int getIsViewed() {
        return isViewed;
    }

    public void setIsViewed(int isViewed) {
        this.isViewed = isViewed;
    }

    public Timestamp getUpdateTime() {
        return updateTime;
    }

    public void setUpdateTime(Timestamp updateTime) {
        this.updateTime = updateTime;
    }
}
