package main

import (
	"context"
	"fmt"
	"github.com/Zengzhx/gotask/models"
	"github.com/go-redis/redis/v8"
	"github.com/gorhill/cronexpr"
	"github.com/jinzhu/gorm"
	_ "github.com/jinzhu/gorm/dialects/mysql"
	"github.com/robfig/cron/v3"
	"net/http"
	_ "net/http/pprof"
	"os"
	"time"
)

var (
	RDB *redis.Client
	DB  *gorm.DB

	TaskKey = "Task_Key"
)

func initRedis() error {
	RDB = redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // 密码
		DB:       0,  // 数据库
		PoolSize: 20, // 连接池大小
	})
	result, err := RDB.Ping(context.TODO()).Result()
	if err != nil {
		fmt.Fprintf(os.Stderr, "redis connect err - Msg:%v", err)
		return err
	}
	fmt.Printf("Ping: %v\n", result)
	return nil
}

func initMysql() error {
	//gorm.Open()
	db, err := gorm.Open("mysql", "root:123456@/test?charset=utf8&parseTime=True&loc=Local")
	if err != nil {
		fmt.Fprintf(os.Stderr, "Open Mysql Err - Msg:%v\n", err)
		return err
	}
	DB = db
	fmt.Println("Connect Mysql Success")
	return nil
}

func initOneTimerTask() *models.TimerJob {
	return &models.TimerJob{
		JobName: "paypal",
		Cron:    "0 0/10 * * * ? *",
		Status:  0,
		Retry:   0,
	}

}

func LoadAllActiveJob(timerJobModel *models.TimerJobModel) (models.TimerJobs, error) {
	data := make(models.TimerJobs, 0)
	getAllActiveJob := func() (models.TimerJobs, error) {
		allJob, err := timerJobModel.GetAllActiveJob()
		if err != nil {
			fmt.Println("数据查询失败")
			return nil, err
		}
		_, err = RDB.Set(context.TODO(), TaskKey, allJob, time.Hour*2).Result()
		if err != nil {
			fmt.Printf("redis存储失败 - Msg:%v\n", err)
			return nil, err
		}
		return allJob, nil
	}
	bytes, err := RDB.Get(context.TODO(), TaskKey).Bytes()
	if err != nil {
		return getAllActiveJob()
	}
	err = data.UnmarshalBinary(bytes)
	if err != nil {
		return getAllActiveJob()
	}
	return data, nil
}

func processTaskJob() {
	fmt.Printf("GOOOOO\n")
}

func main() {
	go func() {
		fmt.Println(http.ListenAndServe("localhost:10000", nil))
	}()

	if err := initMysql(); err != nil {
		return
	}
	if err := initRedis(); err != nil {
		return
	}

	err := DB.AutoMigrate(&models.TimerJob{}).Error
	if err != nil {
		fmt.Println(err)
		return
	}

	timerJobModel := models.DefaultTimerJobModel(DB, RDB)

	// 这一层加redis全局锁，如果某个Pod拿到，则开启定时任务，保证只有一个Pod能有定时任务
	cTask := cron.New(cron.WithSeconds()) //创建一个cron的实例

	_, err = cTask.AddFunc("0 */1 * * * *", func() {
		fmt.Printf("%v :Time Task Alive .....\n", time.Now().Format("2006-01-02 15:04:05"))
		// 加载DB中的有效任务
		jobs, err := LoadAllActiveJob(timerJobModel)
		if err != nil {
			return
		}
		for index, job := range jobs {
			if job.NextTriggerTime == nil || time.Now().After(*job.NextTriggerTime) {
				//
				go processTaskJob()
				cronParse, err := cronexpr.Parse(job.Cron)
				if err != nil {
					fmt.Printf("格式错误 - Msg:%v\n", err)
					continue
				}
				nextTime := cronParse.Next(time.Now())
				now := time.Now()
				jobs[index].NextTriggerTime = &nextTime
				jobs[index].TriggerTime = &now
				err = timerJobModel.UpdateNextTriggerTime(jobs[index])
				if err != nil {
					continue
				}
				//
				RDB.Del(context.TODO(), TaskKey)
			}
		}
	})
	if err != nil {
		return
	}
	cTask.Start()
	defer cTask.Stop()
	select {}
}
