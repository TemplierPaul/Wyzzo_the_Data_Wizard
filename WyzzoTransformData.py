import WyzzoDataGraph as wdg
import pandas as pd


class Transformer:

    def __init__(self, node):
        self.node = node
        self.sql = ''
        self.col = None
        self.fields = []
        self.join_sql = None

    @wdg.debug
    def getColumns(self):
        sql = """SELECT Column_name, data_type
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = '"""
        col = {}
        for i in self.node.input:
            conn = self.node.graph.engine.connect()
            col[i] = pd.read_sql(sql + i.name + "'", con=conn)
            conn.close()
        self.col = col
        return self

    @wdg.debug
    def generateSQL(self, load=False):
        print(self.fields)
        if len(self.fields) == 0:
            self.sql = 'SELECT *'
        else:
            self.sql = 'SELECT ' + self.fields[0]
            for i in range(1, len(self.fields)):
                self.sql += ' ,\n ' + self.fields[i]
        if self.join_sql is None:
            self.sql += ' from ' + self.node.input[0].name
        else:
            self.sql += self.join_sql
        if load:
            self.node.loadSQL(self.sql)
        return self

    def run(self):
        return self.node.run()

    @wdg.debug
    def doMagic(self):
        self.getColumns()
        for i in self.node.input:
            df = self.col[i]
            for i, row in df.iterrows():
                col = row['Column_name']
                print('\n', col, ':', row['data_type'])
                if ' ' in col:
                    col = '`' + col + '`'
                if row['data_type'] == 'text':
                    if col == 'Date':
                        self.parseDate(col=col)
                        print('-> parseDate')
                    else:
                        self.dummies(col=col)
                        print('-> dummies')
                elif 'int' in row['data_type']:
                    self.fields.append(col)
                    print('-> no change')
        return self

    @wdg.debug
    def parseDate(self, col='Date', format='%d/%m/%Y', keep=True, features=True):
        if ' ' in col:
            col = '`' + col + '`'
        if keep: self.fields.append('STR_TO_DATE(' + col + ", '" + format + "') AS Date")
        if features:
            self.fields.append('DAY(STR_TO_DATE(' + col + ", '" + format + "')) AS Day")
            self.fields.append('DAYOFWEEK(STR_TO_DATE(' + col + ", '" + format + "')) AS Weekday")
            self.fields.append('MONTH(STR_TO_DATE(' + col + ", '" + format + "')) AS Month")
            self.fields.append('YEAR(STR_TO_DATE(' + col + ", '" + format + "')) AS Year")
        return self

    @wdg.debug
    def parseDateTime(self, col='Date', format='%d/%m/%Y', keep=True, features=True):
        if ' ' in col:
            col = '`' + col + '`'
        if keep: self.fields.append('STR_TO_DATE(' + col + ", '" + format + "') AS Date")
        if features:
            self.fields.append('DAY(STR_TO_DATE(' + col + ", '" + format + "')) AS Day")
            self.fields.append('DAYOFWEEK(STR_TO_DATE(' + col + ", '" + format + "')) AS Weekday")
            self.fields.append('MONTH(STR_TO_DATE(' + col + ", '" + format + "')) AS Month")
            self.fields.append('YEAR(STR_TO_DATE(' + col + ", '" + format + "')) AS Year")
            self.fields.append('HOUR(STR_TO_DATE(' + col + ", '" + format + "')) AS Hour")
            self.fields.append('MINUTE(STR_TO_DATE(' + col + ", '" + format + "')) AS Minute")
            self.fields.append('SECOND(STR_TO_DATE(' + col + ", '" + format + "')) AS Second")
        return self

    @wdg.debug
    def dummies(self, col, values=None, keep=False):
        c = col
        if ' ' in col:
            col = '`' + col + '`'
        if values is None:
            conn = self.node.graph.engine.connect()
            un = pd.read_sql('SELECT DISTINCT ' + col + ' FROM ' + self.node.input[0].name, con=conn)
            conn.close()
        else:
            un = pd.DataFrame({col: values})
        if keep: self.fields.append(col)
        for i, row in un.iterrows():
            v = str(row[c])
            n = c + "__" + v
            n = n.replace(' ', '_')
            n = n.replace('-', '_')
            n = n.replace("'", '_')
            n = n.replace(".", '')
            n = n.replace("\\", '')
            n = n.replace("/", '')
            self.fields.append(col + ' LIKE "' + v + '" AS ' + n)
        return self

    @wdg.debug
    def join(self, right, on_left, on_right, type='full'):
        print (self.node.graph)
        self.node.input += self.node.graph.toNodes(right)
        right = self.node.graph.toNodes(right)[0]
        self.join_sql = ' from ' + self.node.input[0].name + ' ' + type + ' Join ' + right.name + ' on ' + \
                        self.node.input[0].name + '.' + on_left + ' = ' + right.name + '.' + on_right
        print(self.join_sql)
        return self

    @wdg.debug
    def editData(self, col):
        edit = "select distinct " + col + " as OLD_" + col + ", " + col + " as New_" + col + ' from ' + self.node.input[
            0].name
        self.node.graph.addSQLNode('edit_' + col, input='matches', output_name='new_' + col).loadSQL(edit).run()
        self.getColumns()
        for i, row in self.col[self.node.input[0]].iterrows():
            print(row)
            c = row['Column_name']
            if ' ' in c:
                c = '`' + c + '`'
            if c == col:
                self.fields.append('New_' + col + ' AS ' + col)
            else:
                self.fields.append(c)
        self.join(type='Left', right='new_' + col, on_left=col, on_right="OLD_" + col)
        return self
