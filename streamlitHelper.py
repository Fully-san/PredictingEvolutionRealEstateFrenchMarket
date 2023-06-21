import altair as alt
import polars as pl
import streamlit as st

@st.cache_data(show_spinner=False, experimental_allow_widgets=True)
def loadDataframe():
    return pl.read_csv(source='Data/valeursFoncieres.csv')

def drawTop15(top15Years, default = True, new = True):
    st.write("")
    st.write("Evolution of the price per m² of the 15 largest cities in France between " + top15Years[0] + " and " + top15Years[-1] + " (Without Strasbourg which is in Bas-Rhin).")

    # Rechargement de la page sans aucun changement
    if new:
        # Rechargement de la page avec les settings par défaut
        if default:
            st.session_state['top15_df'] = pl.read_csv(source='./Data/top15melt.csv', separator=',', ignore_errors=True)
        # Chargement de la page avec un changement, avec des settings différents et en rechargeant le top15
        else:
            st.session_state['top15_df'] = pl.read_csv(source='./Data/top15melt.csv', separator=',', ignore_errors=True).filter(pl.col('date').str.slice(0, 4).is_in(top15Years))

    q = (
        st.session_state['top15_df'].lazy()
        .filter(pl.col('date') == top15Years[0] + '-01-01')
        .sort('averageMonthlyPrice', descending=True)
    )
    sortCity = q.collect()['city'].to_numpy()

    selection = alt.selection_point(fields=['city'], bind='legend')

    base = alt.Chart(st.session_state['top15_df'].to_pandas(), title="City TOP 15 ("+ top15Years[0] + "-" + top15Years[-1] +")").encode(
        x=alt.X('date:T',
            axis=alt.Axis(title=' ', tickCount=len(top15Years))
        ),
        y=alt.Y('averageMonthlyPrice:Q',
            scale=alt.Scale(zero=False),
            axis=alt.Axis(title='price per m²')
        ),
        color=alt.Color('city', scale=alt.Scale(scheme='yellowgreenblue', reverse=True), sort=sortCity, legend=alt.Legend(title="")),
        tooltip=['city']
    )

    points = base.mark_circle().encode(
        opacity=alt.value(0)
    )
    
    lines = base.mark_line(interpolate='basis').encode(
        size=alt.value(3),
        opacity=alt.condition(selection, alt.value(1), alt.value(0.2)),
    ).add_params(
        selection
    ).properties(
        height=500
    )

    c = alt.layer(
        lines, points
    )

    st.altair_chart(c, use_container_width=True)