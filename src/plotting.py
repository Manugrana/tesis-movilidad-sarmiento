import matplotlib.pyplot as plt

def save_basic_hist(values, path):
    plt.figure()
    plt.hist(values)
    plt.savefig(path, bbox_inches='tight', dpi=150)
    plt.close()
