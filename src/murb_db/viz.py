"""Visualization helpers — thin wrappers for quick charts."""

from typing import Optional

import matplotlib.pyplot as plt
import pandas as pd


def bar_chart(
    df: pd.DataFrame, x: str, y: str, title: Optional[str] = None, **kwargs
) -> plt.Figure:
    """Create a bar chart from a DataFrame."""
    fig, ax = plt.subplots(figsize=kwargs.pop("figsize", (10, 6)))
    df.plot.bar(x=x, y=y, ax=ax, **kwargs)
    if title:
        ax.set_title(title)
    ax.set_xlabel(x.replace("_", " ").title())
    ax.set_ylabel(y.replace("_", " ").title())
    plt.xticks(rotation=45, ha="right")
    plt.tight_layout()
    return fig


def scatter(
    df: pd.DataFrame, x: str, y: str, color: Optional[str] = None,
    title: Optional[str] = None, **kwargs
) -> plt.Figure:
    """Create a scatter plot from a DataFrame."""
    fig, ax = plt.subplots(figsize=kwargs.pop("figsize", (10, 6)))
    if color and color in df.columns:
        for label, group in df.groupby(color):
            ax.scatter(group[x], group[y], label=label, **kwargs)
        ax.legend(title=color.replace("_", " ").title())
    else:
        ax.scatter(df[x], df[y], **kwargs)
    if title:
        ax.set_title(title)
    ax.set_xlabel(x.replace("_", " ").title())
    ax.set_ylabel(y.replace("_", " ").title())
    plt.tight_layout()
    return fig


def timeseries(
    df: pd.DataFrame, x: str, y: str, title: Optional[str] = None, **kwargs
) -> plt.Figure:
    """Create a time-series line plot."""
    fig, ax = plt.subplots(figsize=kwargs.pop("figsize", (12, 6)))
    plot_df = df.copy()
    plot_df[x] = pd.to_datetime(plot_df[x])
    plot_df = plot_df.sort_values(x)
    ax.plot(plot_df[x], plot_df[y], **kwargs)
    if title:
        ax.set_title(title)
    ax.set_xlabel(x.replace("_", " ").title())
    ax.set_ylabel(y.replace("_", " ").title())
    fig.autofmt_xdate()
    plt.tight_layout()
    return fig
