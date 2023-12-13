from apscheduler.triggers.cron import CronTrigger
from apscheduler.schedulers.blocking import BlockingScheduler
from cron import(
    migrate_data,
    delete_mysql,
)

scheduler = BlockingScheduler()

TIME_ZONE = 'Asia/Shanghai'

job = scheduler.add_job(func=migrate_data.run,
                        kwargs=None,
                        trigger=CronTrigger.from_crontab(
                            "0 * * * *", timezone=TIME_ZONE),
                        id="migrate_data",
                        name="migrate_data",
                        replace_existing=True,
                        misfire_grace_time=1)

# job = scheduler.add_job(func=delete_mysql.run,
#                         kwargs=None,
#                         trigger=CronTrigger.from_crontab(
#                             "4 * * * *", timezone=TIME_ZONE),
#                         id="migrate_data",
#                         name="migrate_data",
#                         replace_existing=True,
#                         misfire_grace_time=1)

if __name__ == "__main__":
    scheduler.start()