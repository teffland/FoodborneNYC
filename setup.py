from distutils.core import setup
setup(name='haystack',
      version='0.1',
      description='Python DOHMH Foodborne Illness Software',
      author='Tom Effland',
      author_email='teffland@cs.columbia.edu',
      #url='',
      packages=['haystack', 
                'haystack.data_models',
                'haystack.sources',
                #'haystack.pipelines',
                #'haystack.pipes',
                'haystack.util',
                'haystack.methods',
                #'haystack.experiments',
                #'haystack.deployment'
               ],
      )
