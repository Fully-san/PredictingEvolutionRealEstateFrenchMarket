import polars as pl
import datetime as dt
from scipy.stats import linregress

# Update with new data
years = ['2014','2015','2016','2017','2018','2019','2020','2021','2022']

columnList = ['Code departement',
              'Commune',
              'Date mutation',
              'Nature mutation',
              'Nombre de lots',
              'Surface Carrez du 1er lot',
              'Type local',
              'Valeur fonciere']

def generateValeurFoncieres(fileList):
    df = pl.DataFrame(data=[], schema={
                            "departmentCode": pl.Int64,
                            "city": pl.Utf8,
                            "date": pl.Utf8,
                            "typeTransaction": pl.Utf8,
                            "area": pl.Float64,
                            "typeLocal": pl.Utf8,
                            "price": pl.Int32,
                            "pricePerM²": pl.Float64
                        })

    for file in fileList:
        q = (pl.scan_csv(source=file, separator='|', ignore_errors=True)
            .select(columnList)
            .rename({"Code departement": "departmentCode",
                    "Commune": "city",
                    "Date mutation": "date",
                    "Nature mutation": "typeTransaction",
                    "Valeur fonciere": "price",
                    "Surface Carrez du 1er lot": "area",
                    "Type local":"typeLocal"
            })
            .filter(pl.col('typeTransaction').is_in(['Vente', "Vente en l'état futur d'achèvement"])
                        & pl.col('Nombre de lots') >= 1
            )
            .drop('Nombre de lots')
            .drop_nulls()
            .with_columns(pl.col('price').str.replace(r",[0-9]*",'').cast(pl.Int32, strict=False))
            .with_columns(pl.col('area').str.replace(',', '.').cast(pl.Float32, strict=False).apply(lambda x: round(x, 2)))
            .with_columns(pl.col("date").str.slice(6, 4) + '-' + pl.col("date").str.slice(3, 2))
            .unique(keep="first")
            .with_columns((pl.col('price') / pl.col('area')).apply(lambda x: round(x, 2)).alias('pricePerM²'))
        )

        df = pl.concat([df, q.collect()])
        
    df = df.filter(  (pl.col('price') < pl.col('price').quantile(0.99, 'higher'))
                & (pl.col('price') > pl.col('price').quantile(0.01, 'lower'))
                & (pl.col('area') < pl.col('area').quantile(0.99, 'higher'))
                & (pl.col('area') > pl.col('area').quantile(0.01, 'lower'))
                & (pl.col('pricePerM²') < pl.col('pricePerM²').quantile(0.99, 'higher'))
                & (pl.col('pricePerM²') > pl.col('pricePerM²').quantile(0.01, 'lower'))
        )

    mapper = {
        'LYON 1ER': 'LYON',
        'LYON 2EME': 'LYON',
        'LYON 3EME': 'LYON',
        'LYON 4EME': 'LYON',
        'LYON 5EME': 'LYON',
        'LYON 6EME': 'LYON',
        'LYON 7EME': 'LYON',
        'LYON 8EME': 'LYON',
        'LYON 9EME': 'LYON',
        'MARSEILLE 1ER': 'MARSEILLE',
        'MARSEILLE 2EME': 'MARSEILLE',
        'MARSEILLE 3EME': 'MARSEILLE',
        'MARSEILLE 4EME': 'MARSEILLE',
        'MARSEILLE 5EME': 'MARSEILLE',
        'MARSEILLE 6EME': 'MARSEILLE',
        'MARSEILLE 7EME': 'MARSEILLE',
        'MARSEILLE 8EME': 'MARSEILLE',
        'MARSEILLE 9EME': 'MARSEILLE',
        'MARSEILLE 10EME': 'MARSEILLE',
        'MARSEILLE 11EME': 'MARSEILLE',
        'MARSEILLE 12EME': 'MARSEILLE',
        'MARSEILLE 13EME': 'MARSEILLE',
        'MARSEILLE 14EME': 'MARSEILLE',
        'MARSEILLE 15EME': 'MARSEILLE',
        'MARSEILLE 16EME': 'MARSEILLE',
        'PARIS 01': 'PARIS',
        'PARIS 02': 'PARIS',
        'PARIS 03': 'PARIS',
        'PARIS 04': 'PARIS',
        'PARIS 05': 'PARIS',
        'PARIS 06': 'PARIS',
        'PARIS 07': 'PARIS',
        'PARIS 08': 'PARIS',
        'PARIS 09': 'PARIS',
        'PARIS 10': 'PARIS',
        'PARIS 11': 'PARIS',
        'PARIS 12': 'PARIS',
        'PARIS 13': 'PARIS',
        'PARIS 14': 'PARIS',
        'PARIS 15': 'PARIS',
        'PARIS 16': 'PARIS',
        'PARIS 17': 'PARIS',
        'PARIS 18': 'PARIS',
        'PARIS 19': 'PARIS',
        'PARIS 20': 'PARIS',
    }

    df = df.with_columns(pl.col('city').map_dict(mapper, default=pl.first()))
    df = df.sort('date', descending=False)
    df.write_csv('Data/valeursFoncieres.csv', separator=',')


def createCityDataframe(df, listCodeDepartement, listCity, listTypeLocal = ['Appartement', 'Maison']):
    df = df.lazy().filter(pl.col('departmentCode').is_in(listCodeDepartement)
                    & pl.col('city').is_in(listCity)
                    & pl.col('typeLocal').is_in(listTypeLocal)
    )

    return df.collect()

def createAverageM2PriceEWMA12Dataframe(df, years):
    q = (
        df.lazy()
        .filter(pl.col('date').str.slice(0, 4).is_in(years))
        .groupby('date').agg(pl.col("pricePerM²").mean().round(2))
        .with_columns((pl.col('date').apply(lambda x: dt.datetime.strptime(x, '%Y-%m').toordinal()).alias('ordinalDate')))
        .with_columns(pl.col('date').str.strptime(pl.Date, '%Y-%m', strict=False))
        .sort('date', descending=False)
        .drop_nulls()
        .with_columns((pl.col('pricePerM²').ewm_mean(span=12).round(2)).alias('averageMonthlyPriceEWMA-12'))
        .select(pl.col('date'), pl.col('ordinalDate'), pl.col('pricePerM²'), pl.col('averageMonthlyPriceEWMA-12'))
    )

    return q.collect()

def createTop15CityMeltDataframe(path, years, listCodeDepartementTop15, listCityTop15):
    q = (
        pl.scan_csv(source=path, separator=',')
        .filter(pl.col('date').str.slice(0, 4).is_in(years)
                & pl.col('departmentCode').is_in(listCodeDepartementTop15)
                & pl.col('city').is_in(listCityTop15)
        )
        .groupby(['date','city']).agg(pl.col("pricePerM²").mean().round(2).alias('averageMonthlyPrice'))
        .with_columns((pl.col('date').apply(lambda x: dt.datetime.strptime(x, '%Y-%m').toordinal()).alias('ordinalDate')))
        .with_columns(pl.col('date').str.strptime(pl.Date, '%Y-%m', strict=False))
        .sort('city', 'date', descending=False)
        .drop_nulls()
        .select(pl.col('city'), pl.col('date'), pl.col('ordinalDate'), pl.col('averageMonthlyPrice'))
    )

    new_df = pl.DataFrame(data=[], schema={
                         "city": pl.Utf8,
                         "date": pl.Date,
                         "averageMonthlyPrice": pl.Float64
                    })

    for name, data in q.collect().groupby('city'):
        data = data.with_columns((pl.col('averageMonthlyPrice').ewm_mean(span=12)).round(2).alias('averageMonthlyPriceEWMA-12'))
        res = linregress(data['ordinalDate'], data['averageMonthlyPriceEWMA-12'])
        data = data.with_columns((res.slope * pl.col('ordinalDate')+ res.intercept).round(2).alias('averageMonthlyPrice'))
        data = data.select(pl.col('city'), pl.col('date'), pl.col('averageMonthlyPrice'))
        
        new_df = pl.concat([new_df, data])

    return new_df
