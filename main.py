import WyzzoDataGraph as wdg
import WyzzoTransformData as wtd

def create():
    G = wdg.Graph('db')\
        .connect()
    G.addDataNode('matches').loadCSV('matches.csv')
    G.addDataNode('iris').loadCSV('iris.csv')
    wtd.Transformer(node=G.addSQLNode(name='transfo', input='matches', output_name='transformed'))\
        .editData(col='Competition')\
        .generateSQL(load=True)\
        .run()
    wtd.Transformer(node=G.addSQLNode(name='final_transfo', input='transformed', output_name='final')) \
        .doMagic() \
        .generateSQL(load=True) \
        .run()
    G.saveConfig('config.json')
    return G

def load():
    X = wdg.Graph().loadConfig('config.json')
    return X

