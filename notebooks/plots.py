# Plotting
import plotly.graph_objs as go

# Setting cufflinks
import textwrap
import cufflinks as cf
cf.go_offline()
cf.set_config_file(offline=False, world_readable=True)

# Centering and fixing title
def iplottitle(title, width=40):
    return '<br>'.join(textwrap.wrap(title, width))

# Adding custom colorscales (to add one: themes/custom_colorscales.yaml)\n",
import yaml
custom_colorscales = yaml.load(open('themes/custom_colorscales.yaml', 'r'))
cf.colors._custom_scales['qual'].update(custom_colorscales)
cf.colors.reset_scales()

# Setting cuffilinks template (use it with .iplot(theme='custom')
cf.themes.THEMES['custom'] = yaml.load(open('themes/cufflinks_template.yaml', 'r'))

import numpy as np


def plot_heatmap(df, x, y, z, title, colorscale='oranges'):

    return df.pivot(columns=y,
                    index=x,
                    values=z).iplot(kind='heatmap', 
                                    theme='custom',
                                    colorscale=colorscale, 
                                    title=title)


def plot_rt(t, title):
    
    # TODO: put dicts in config
    rt_classification = {
        'Risco médio: Acima desta linha, cada 10 pessoas infectam em média entre outras 10-12': {'threshold': 1,
                  'color': 'rgba(132,217,217,1)',
                  'fill': None,
                  'width': 3},
        'Risco alto: Acima desta linha, cada 10 pessoas infectam em média mais de 12 outras': {'threshold': 1.2,
                'color': 'rgba(242,185,80,1)',
                'fill': None,
                'width': 3},
    }

    ic_layout = {
            'Rt_high_95': {
                'fill': 'tonexty', 
                'showlegend': False, 
                'name': None,
                'layout': {
                    'color': '#E5E5E5', 
                    'width': 2
                }
            },
            'Rt_low_95' : {
                'fill': 'tonexty', 
                'showlegend': True, 
                'name': 'Intervalo de confiança - 95%',
                'layout': {
                    'color': '#E5E5E5', 
                    'width': 2
                }
            },
            'Rt_most_likely' : {
                'fill': None, 
                'showlegend': True, 
                'name': 'Valor médio <b>(atual={})'.format(10*t['Rt_most_likely'].iloc[-1]),
                'layout': {
                    'color': 'rgba(63, 61, 87, 0.8)', 
                    'width': 3
                }
            },
    }

    fig = go.Figure()

    # Intervalos de confianca
    for bound in ic_layout.keys():

        fig.add_scattergl(
            x=t['last_updated'], 
            y=t[bound].apply(lambda x: 10*x), 
            line=ic_layout[bound]['layout'],
            fill=ic_layout[bound]['fill'],
            mode='lines',
            showlegend=ic_layout[bound]['showlegend'],
            name=ic_layout[bound]['name']
        )

    # Areas de risco
    for bound in rt_classification.keys():

        fig.add_trace(go.Scatter(
            x=t['last_updated'],
            y=[rt_classification[bound]['threshold']*10 for i in t['last_updated']],
            line={'color': rt_classification[bound]['color'], 
                  'width': rt_classification[bound]['width'], 
                  'dash': 'dash'}, # 0
            fill=rt_classification[bound]['fill'],
            name=bound,
            showlegend=[False if bound == 'zero' else True][0]
       ))

    fig.layout.yaxis.rangemode = 'tozero'
    # fig.layout.yaxis.range = [0,5]

    fig.update_layout(template='plotly_white', 
                      title=title)
    return fig


def plot_rt_bars(df, title, place_type='state'):
    
    df['color'] = np.where(df['Rt_most_likely'] > 1.2,
                           'rgba(242,185,80,1)',
                           np.where(df['Rt_most_likely'] > 1, 
                                    'rgba(132,217,217,1)',
                                    '#0A96A6'))

    fig = go.Figure(go.Bar(x=df[place_type],
                          y=df['Rt_most_likely'],
                          marker_color=df['color'],
                          error_y=dict(
                            type='data',
                            symmetric=False,
                            array=df['Rt_most_likely'] - df['Rt_low_95'],
                            arrayminus=df['Rt_most_likely'] - df['Rt_low_95'])))
    
    fig.add_shape(
        # Line Horizontal
            type="line",
            x0=-1,
            x1=len(df[place_type]),
            y0=1,
            y1=1,
            line=dict(
                color="#E5E5E5",
                width=2,
                dash="dash",
            ),
    )

    fig.update_layout({'template': 'plotly_white', 
                       'title': title})
    
    return fig


# def plot_rt(df_uf)
#     t = df_uf.reset_index()#.set_index('date')
#     t['date'] = pd.to_datetime(t['date'])

#     t['group'] = np.where(t['ML'] >= 1.2, 'high', np.where(t['ML'] >= 1, 'medium', 'low'))
#     t['color'] = t['group'].map({'high': '#F26430', 
#                                  'medium':'#F2CD13', 
#                                  'low': '#435159'})

#     cols = {'ML': 'Rt estimado',
#             'Low_90': 'min IC-90%',
#             'High_90': 'max IC-90%',
#             'group': 'group'}

#     t = t.rename(cols, axis=1)

#     fig = go.Figure()

#     error_color = '#E5E5E5'

#     fig.add_trace(
#         go.Scatter(
#             x=t['date'], 
#             y=t['min IC-90%'], 
#             mode='lines', 
#             # fill='tonexty',
#             line_color=error_color,
#             showlegend=False
#         )
#     )

#     fig.add_trace(
#         go.Scatter(
#             x=t['date'], 
#             y=t['max IC-90%'], 
#             mode='lines', 
#             fill='tonexty',
#             line_color=error_color,
#             name='IC-90%'
#         )
#     )


#     fig.add_trace(
#         go.Scatter(
#             x=t['date'], 
#             y=t['Rt estimado'], 
#             mode='markers+lines',
#             line_color='#898F8F',
#             marker=dict(size=12,
#                        color=t['color']),
#             name='Rt estimado'
#         )
#     )


#     fig.update_layout(yaml.load(open('themes/cufflinks_template.yaml', 'r'))['layout'])
#     fig.update_layout({'title': 'Número de reprodução básico COVID-19 - {}'.format(t['state'].unique()[0])})
    
#     return fig