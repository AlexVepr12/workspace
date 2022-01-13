import plotly.express as px
import pandas as pd

df = pd.read_csv('feeds_all.csv', header=0)
df['q'] = 1

# Количество публикаций по месяцам
df['date'] = pd.to_datetime(df['updated_time'])
monthly_quality = df.groupby(pd.Grouper(key='date', freq='M'))['q'].sum().fillna(0)
df_m = monthly_quality.to_frame()
df_m['date'] = df_m.index
# визуализация частоты постов
fig = px.line(df_m, x='date', y='q', title='Количество публикаций')

fig.update_xaxes(
    rangeslider_visible=True,
    rangeselector=dict(
        buttons=list([
            dict(count=1, label="1m", step="month", stepmode="backward"),
            dict(count=6, label="6m", step="month", stepmode="backward"),
            dict(count=1, label="YTD", step="year", stepmode="todate"),
            dict(count=1, label="1y", step="year", stepmode="backward"),
            dict(step="all")
        ])
    )
)
fig.show()

# Количество публикаций по мперсонам
person_q = df.groupby(pd.Grouper(key='author_name'))['q'].sum().fillna(0)
sum_p = person_q.sum()
person_q /= (sum_p/100)

df_p = person_q.to_frame()
df_p['author_name'] = df_p.index

# визуализация авторов
df_p.loc[df_p['q'] < 0.5, 'author_name'] = 'Другие'
fig = px.pie(df_p, values='q', names='author_name')
fig.show()