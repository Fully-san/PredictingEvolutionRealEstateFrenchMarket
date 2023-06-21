import datetime as dt
import altair as alt
import streamlit as st
from scipy.stats import linregress

from helper import *
from streamlitHelper import *

if 'oldYears' not in st.session_state:
    st.session_state['oldYears'] = []

st.set_page_config(layout="wide")
st.title('Evolution of the real estate market in France')

st.write("Source of data: [Dataset] (https://www.data.gouv.fr/fr/datasets/demandes-de-valeurs-foncieres/).")
st.write("The data cover the entire territory of metropolitan France, except for the departments of Bas-Rhin (67), Haut-Rhin (68) and Moselle (57).")

with st.form(key='optionsForm'):
    with st.sidebar:
        st.title('Options')

        startYear, oldYears = st.select_slider(
            'Select a range of date',
            options=years,
            value=(years[0], years[-1])
            )

        selectedYears = [str(x) for x in list(range(int(startYear), int(oldYears) + 1))]

        typeLocal = st.multiselect('Select the type of property you want:', ['Apartment', 'House', 'Outbuilding', 'Commercial premises'], default=['Apartment', 'House'], key='typeLocal')
        cities = st.text_area("Enter the different cities by going to the line:", value='', key="listCity")

        codes = [i for i in range(1, 96) if i not in [55, 67, 68] ]
        listCodeDepartment = st.multiselect('Select the code of the department where your cities are located', codes)

        submit = st.form_submit_button('Submit')

drawTop15(selectedYears, years == selectedYears, st.session_state['oldYears'] != selectedYears)

if submit:
    st.session_state['oldYears'] = selectedYears
    listTypeLocal = []
    if 'Apartment' in typeLocal:
        listTypeLocal.append('Appartement')
    if 'House' in typeLocal:
        listTypeLocal.append('Maison')
    if 'Outbuilding' in typeLocal:
        listTypeLocal.append('Dépendance')
    if 'Commercial premises' in typeLocal:
        listTypeLocal.append('Local industriel. commercial ou assimilé')

    listCity = [x.upper() for x in cities.split('\n')]

    city_df = createCityDataframe(loadDataframe(), listCodeDepartment, listCity, listTypeLocal).sort('date', descending=False)

    if city_df.is_empty():
        st.title("There are no results for your search.")
    else:
        df = createAverageM2PriceEWMA12Dataframe(city_df, selectedYears)

        res = linregress(df['ordinalDate'], df['averageMonthlyPriceEWMA-12'])
        df = df.with_columns((res.slope * pl.col('ordinalDate')+ res.intercept).round(2).alias('linearRegression'))
        df = df.drop("ordinalDate")
        df = df.melt(id_vars='date', variable_name='category', value_name='pricePerM²')

        title = "Evolution of the average monthly price per m² for the cities of"
        for city in listCity:
            title += ' ' + city.capitalize()

        line = alt.Chart(df.to_pandas(), title=title).mark_line(interpolate='basis').encode(
            x=alt.X('date:T',
                axis=alt.Axis(title=' ', tickCount=len(selectedYears))
            ),
            y=alt.Y('pricePerM²:Q',
                scale=alt.Scale(zero=False),
                axis=alt.Axis(title='price per m²')
            ), 
            color=alt.Color('category', scale=alt.Scale(scheme='yellowgreenblue'), legend=alt.Legend(title=""), sort=['averageMonthlyPrice', 'averageMonthlyPriceEWMA-12', 'linearRegression'])
        ).properties(
            height=400
        )

        c = alt.layer(
            line
        )

        st.altair_chart(c, use_container_width=True)

        text = ""
        for i in range(2015, 2031, 1):
            text += 'On the first January <span class="textColor">' + str(i) + '</span> the average price per m² for the cities of'
            for city in listCity:
                text += ' ' + city.capitalize()
            text += ' is estimated at '

            text += '<span class="textColor">' + str(round(res.slope * dt.datetime(i, 1, 1).toordinal() + res.intercept)) + '</span> €.\n\n'

        st.markdown("""<style>.textColor {color: #45b4c2 !important;}</style>""", unsafe_allow_html=True)
        st.markdown(text, unsafe_allow_html=True)
