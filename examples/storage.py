from tableschema_spss import Storage

storage = Storage(base_path='tmp')
storage.create('persons.sav', {
    'fields': [
        {
            'name': 'person_id',
            'type': 'integer',
            'spss:format': 'F8'
        },
        {
            'name': 'name',
            'type': 'string',
            'spss:format': 'A10'
        },
        {
            'type': 'number',
            'name': 'salary',
            'title': 'Current Salary',
            'spss:format': 'DOLLAR8'
        },
        {
           'type': 'date',
           'name': 'bdate',
           'title': 'Date of Birth',
           'format': '%Y-%m-%d',
           'spss:format': 'ADATE10'
        }
    ]
})
storage.write('persons.sav', [
    ['1', 'Alex', '1000', '2015-02-02'],
    ['2', 'John', '2000', '2015-03-03'],
    ['3', 'Paul', '3000', '2015-04-04'],
])
print(storage.read('persons.sav'))
