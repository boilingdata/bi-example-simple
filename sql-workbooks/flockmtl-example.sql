-- Press Ctrl/Cmd + Enter to run
SHOW ALL TABLES;


INSTALL flockmtl FROM community;
LOAD flockmtl;

CREATE SECRET (
    TYPE OLLAMA,
    API_URL '127.0.0.1:11434'
);

CREATE MODEL('llama-model', 'llama3.2', 'ollama', {"context_window": 128000, "max_output_tokens": 2048});


