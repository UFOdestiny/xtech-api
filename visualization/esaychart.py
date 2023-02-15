# -*- coding: utf-8 -*-
# @Name     : esaychart.py
# @Date     : 2023/2/15 13:57
# @Auth     : Yu Dahai
# @Email    : yudahai@pku.edu.cn
# @Desc     :

import datetime, psutil
import asyncio
from fastapi import FastAPI
from easycharts import ChartServer
from easyschedule import EasyScheduler

scheduler = EasyScheduler()
app = FastAPI()

every_minute = '* * * * *'


@app.on_event('startup')
async def setup():
    asyncio.create_task(scheduler.start())
    app.charts = await ChartServer.create(
        app,
        charts_db="charts_database",
        chart_prefix='/mycharts'
    )

    await app.charts.create_dataset(
        "test",
        labels=['a', 'b', 'c', 'd'],
        dataset=[1, 2, 3, 4]
    )

    # set initial sync time
    label = datetime.datetime.now().isoformat()[11:19]
    await app.charts.create_dataset(
        'cpu',
        labels=[label],
        dataset=[psutil.cpu_percent()]
    )
    await app.charts.create_dataset(
        'mem',
        labels=[label],
        dataset=[psutil.virtual_memory().percent]
    )

    @scheduler(schedule=every_minute)
    async def resource_monitor():
        time_now = datetime.datetime.now().isoformat()[11:19]

        # updates CPU & MEM datasets with current time
        await app.charts.update_dataset(
            'cpu',
            label=time_now,
            data=psutil.cpu_percent()
        )
        await app.charts.update_dataset(
            'mem',
            label=time_now,
            data=psutil.virtual_memory().percent
        )


if __name__ == "__main__":
    import uvicorn

    uvicorn.run('easychart:app', host='127.0.0.1', port=8000, reload=True, debug=True, workers=1)
