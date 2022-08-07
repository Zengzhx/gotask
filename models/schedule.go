package models

import (
	"encoding/json"
	"fmt"
	"github.com/go-redis/redis/v8"
	"github.com/jinzhu/gorm"
	"os"
	"time"
)

type TimerJob struct {
	Id              int64      `gorm:"COLUMN:id;AUTO_INCREMENT;PRIMARY_KEY"`
	JobName         string     `gorm:"COLUMN:job_name"`
	Cron            string     `gorm:"COLUMN:cron;NOT NULL;COMMENT:'cron表达式'"`
	Status          int        `gorm:"COLUMN:status;DEFAULT '0';NOT NULL;COMMENT:'状态 0=就绪 1=启动 2=暂停 3=停止'"`
	Retry           int        `gorm:"COLUMN:retry;DEFAULT '0';NOT NULL"`
	TriggerTime     *time.Time `gorm:"COLUMN:trigger_time"`
	NextTriggerTime *time.Time `gorm:"COLUMN:next_trigger_time"`
	CreatedAt       *time.Time `gorm:"COLUMN:created_at;DEFAULT CURRENT_TIMESTAMP;NOT NULL"`
	UpdatedAt       *time.Time `gorm:"COLUMN:updated_at;DEFAULT CURRENT_TIMESTAMP;NOT NULL"`
}

type TimerJobs []*TimerJob

func (j TimerJobs) MarshalBinary() (data []byte, err error) {
	return json.Marshal(j)
}

func (j *TimerJobs) UnmarshalBinary(data []byte) error {
	return json.Unmarshal(data, j)
}

func (j *TimerJob) TableName() string {
	return "task_job"
}

type TimerJobModel struct {
	db *gorm.DB      `gorm:"-"`
	rd *redis.Client `gorm:"-"`
}

func DefaultTimerJobModel(db *gorm.DB, rd *redis.Client) *TimerJobModel {
	return &TimerJobModel{
		db: db,
		rd: rd,
	}
}

func (j *TimerJobModel) CreateTimerJob(data *TimerJob) error {
	if err := j.db.Model(&TimerJob{}).Create(&data).Error; err != nil {
		fmt.Fprintf(os.Stderr, "创建 Timer_Job Err - Msg:%v", err)
		return err
	}
	return nil
}

func (j *TimerJobModel) GetAllActiveJob() (TimerJobs, error) {
	allJob := make(TimerJobs, 0)
	if err := j.db.Model(&TimerJob{}).Where("status IN (0, 1)").Scan(&allJob).Error; err != nil {
		fmt.Fprintf(os.Stderr, "GetAllActiveJob Err - Msg:%v", err)
		return nil, err
	}
	return allJob, nil
}

func (j *TimerJobModel) UpdateNextTriggerTime(data *TimerJob) error {
	if err := j.db.Model(&TimerJob{}).Save(&data).Error; err != nil {
		fmt.Fprintf(os.Stderr, "update next trigger time Err - Msg:%v", err)
		return err
	}
	return nil
}
