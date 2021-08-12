import pandas as pd

class OverwriteNotAllowed(Exception):
    """
    Raised when a value corresponding to a key already present in the database
    is entered.
    
    Example:
        db = Database('temp')
        db.temp.insert({1:2, 3:4, 5:6})
        db.temp.insert({1:7}) <---- exception will be raised
    """
        
    pass

class Column:
    '''
    Class defining specific column of the Database class.
    
    variables: name
    variable type: str
    required packages: pandas, sys
    
    functions:
        insert, delete, mean, transaction, rollback, commit
    
    Example:
        col = Column('temp') ... initiates Column object
        col.insert({10:1, 20:2, 30:3}) ... inserts some values 
        col.delete([20]) ... delete a value
        print(col) ... print result
    '''
    
    def __init__(self, name):
        self.name = name  # storing name of the column
        self._vals = {}  # dict serving as a result column
        self.transaction_marker = False  # True: transaction is ON, False: transaction is OFF
        self.trans_dict = {}  # temporary dict for storing data during transaction
        self.to_del = []  # list of keys to be deleted (used during transaction)
        
    def __str__(self):
        return "{}".format(pd.DataFrame({self.name: self._vals}))
    
    def __getitem__(self, key):  #enables subscription ---> db.temperature[20]
        return self._vals[key]
                 
    def insert(self, values):
        '''
        Method insert(self, values) serves to insert data specified in values into the column.
        
        Parameters
        ----------
        values : dict
            Values assigned to specific keys to be inserted into the database.

        Raises
        ------
        OverwriteNotAllowed
            Raised when a value corresponding to a key already present in the database
    is entered. 

        Returns
        -------
        None.

        '''
        
        for key in values.keys():
            if key in self._vals.keys():  # if one of the keys entered is already present an error will be raised
                raise OverwriteNotAllowed('Overwrite not allowed. Key = ' + str(key) + ' already assigned.')
                
        if self.transaction_marker:   # if transaction is on only trans_dict will be updated (temporary dict)
            self.trans_dict.update(values)
        else:
            self._vals.update(values)  # if non-transaction self._vals are directly updated
                    
    def delete(self, keys):
        '''
        Method delete(self, keys) serves to delete data already present in the database characterized
        by their key/s.

        Parameters
        ----------
        keys : list of int
            Keys specifying the values we wish to delete.

        Returns
        -------
        None.

        '''
        if self.transaction_marker: # if transaction is ON
            for key in keys:
                if key in self.trans_dict.keys():  
                    self.trans_dict.pop(key)
                elif key in self._vals.keys():
                    self.to_del.append(key)
        else:                         # transaction OFF
            for key in keys:
                if key in self._vals.keys():
                    self._vals.pop(key)
        
    def mean(self):
        return pd.DataFrame({self.name: self._vals}).mean()[0] 

    def transaction(self):
        self.transaction_marker = True
        
    def rollback(self):
        self.trans_dict.clear()
        self.to_del.clear()
        self.transaction_marker = False
        
    def commit(self):
        self._vals.update(self.trans_dict)
        
        for key in self.to_del:
            self._vals.pop(key)
            
        self.trans_dict.clear()
        self.to_del.clear()
        self.transaction_marker = False
        

class Database(Column):
    """
    Class defining a database composed of Column objects as columns.
    
    variables: tables ... serves as column name/s
    variable type: list of strings
    
    Utilizes functions of Column object.
    
    Example:
        db = Database('temp', 'pres')
        db.temp.insert({1:2, 3:4})
        db.pres.isnert({4:5, 6:7})
        print(db)
    """
    
    def __init__(self, tables):
        self._vars = {}
        self.tables = tables
        
        for a in tables:
            self.__dict__[a] = Column(a)
            
        
    def __str__(self):
        for var in self.tables:
            self._vars.update({var: self.__dict__[var]._vals})
        return "{}".format(pd.DataFrame(self._vars))
        
            

if __name__ == '__main__':
    db = Database(tables=['temperature', 'pressure'])

    # Insert three items immediately
    db.temperature.insert({10: 10, 20: 11, 30: 9})
    print('Mean temperature=', db.temperature.mean(), '- expected value=10')
    print('Temperature at "20"=', db.temperature[20], '- expected value=11')

    # Begin a new transaction
    db.temperature.transaction()

    # Insert two items - queued
    db.temperature.insert({40: 19, 50: 21})

    # Delete one - queued
    db.temperature.delete([10])
    print('Mean temperature=', db.temperature.mean(), '- expected value=10')

    # Discard all operations in the current transaction
    db.temperature.rollback()
    print('Mean temperature=', db.temperature.mean(), '- expected value=10')

    # Start a new one
    db.temperature.transaction()

    # Do some operations...
    db.temperature.insert({40: 0, 50: 2})
    db.temperature.delete([20, 30])
    print('Mean temperature=', db.temperature.mean(), '- expected value=10')

    # Write them to the database
    db.temperature.commit()
    print('Mean temperature=', db.temperature.mean(), '- expected value=4')

    # Attempt to insert a new value with already existing index value - will raise an error
    db.temperature.insert({10: 20})
