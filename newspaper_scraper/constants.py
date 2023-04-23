NLP_ATTRIBUTES = ['text', 'i', 'lemma_', 'pos_', 'tag_', 'dep_', 'shape_', 'is_stop', 'left_edge',
                  'right_edge', 'morph', 'sentiment', 'is_alpha', 'is_digit', 'lang_']

COLUMN_SQL_TYPES = {
    'url': 'TEXT'
}

COLUMN_SQL_TYPES.update({attr: 'BLOP' for attr in NLP_ATTRIBUTES})
