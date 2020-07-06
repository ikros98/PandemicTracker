import sparql
import matplotlib.pyplot as plt
import numpy as np
from scipy.ndimage import gaussian_filter1d
import urllib.parse
import io
from query import *


def prepare_dates(row):
    months = ["Gen", "Feb", "Mar", "Apr", "Mag", "Giu",
              "Lug", "Ago", "Set", "Ott", "Nov", "Dic"]
    date = row[0][5:]
    m = date[:2]
    d = date[3:]
    return d + " " + months[int(m) - 1]


def prepare_values(row):
    row[1] = float(row[1])
    return row[1:]


def plot_for(province, station, observations):

    province_uri, region_uri, province_dist = province
    station_uri, station_dist = station

    province_name = urllib.parse.unquote_plus(province_uri.rsplit('/', 1)[-1])
    region_name = urllib.parse.unquote_plus(region_uri.rsplit('/', 1)[-1])
    station_name = urllib.parse.unquote_plus(station_uri.rsplit('/', 1)[-1])

    observations = [sparql.unpack_row(row) for row in observations.fetchall()]
    dates = [prepare_dates(obs) for obs in observations]
    values = [prepare_values(obs) for obs in observations]

    dates = np.array(dates)
    values = np.array(values)

    infections_data = gaussian_filter1d(
        np.clip(np.diff(values[:, 1], prepend=0),
                a_min=0, a_max=None), 3)
    air_quality_data = gaussian_filter1d(values[:, 0], 3)

    values = values[:, 2:]
    # replace apple nan values (11-12 may) with the preceding values
    apple = values[:, 0]
    mask = np.isnan(apple)
    idx = np.where(~mask, np.arange(mask.shape[0]), 0)
    np.maximum.accumulate(idx, axis=0, out=idx)
    apple[mask] = apple[idx[mask]]

    values[:, 0] = apple - 100
    values[:, 6] = -values[:, 6]
    values = np.average(values, axis=1)
    mobility_data = gaussian_filter1d(values, 2)

    plt.style.use("seaborn-dark")
    for param in ['figure.facecolor', 'axes.facecolor', 'savefig.facecolor']:
        plt.rcParams[param] = '#171d33'
    for param in ['text.color', 'axes.labelcolor', 'xtick.color', 'ytick.color']:
        plt.rcParams[param] = '0.9'

    fig, host = plt.subplots()
    air = host.twinx()
    mob = host.twinx()

    p1, = host.plot(dates, infections_data, color='#08F7FE',
                    linewidth=2, label="Casi giornalieri")
    p2, = air.plot(dates, air_quality_data, color='#FE53BB',
                   linewidth=1, label="PM10", alpha=0.8)
    p3, = mob.plot(dates, mobility_data, color='#00ff41',
                   linewidth=1, label="Mobilità", alpha=0.8)
    plt.xticks(ticks=range(0, len(dates), 7))

    host.fill_between(x=dates,
                      y1=infections_data,
                      y2=[0] * len(infections_data),
                      color='#08F7FE',
                      alpha=0.1)

    host.set_xlabel("Dati sulla mobilità forniti da Apple e Google per la Regione {}.\n".format(region_name) +
                    "Dati sulla qualità dell'aria registrati dalla stazione {} a circa {:.0f}km da te. I dati sono stati traslati di due settimane.".format(
                        station_name, max(1, station_dist)),
                    fontsize=7,
                    position=(1, 0),
                    horizontalalignment='right')
    host.set(title='Provincia di {}'.format(province_name))

    host.tick_params(axis='x', labelsize=8, labelrotation=60)
    host.tick_params(axis='y', labelsize=8)

    host.set_zorder(3)
    host.patch.set_visible(False)

    air.set_yticklabels([])
    mob.set_yticklabels([])

    host.grid(color='#2A3459', linestyle='dotted')

    lines = [p1, p2, p3]
    host.legend(lines, [l.get_label() for l in lines], fontsize=8)

    fig.tight_layout()

    buf = io.BytesIO()
    fig.savefig(buf, dpi=200, format='png')
    buf.seek(0)
    return buf
