{
  "targets": [
    {
      "target_name": "stoolap",
      "sources": ["src/stoolap.c"],
      "include_dirs": [],
      "cflags": ["-std=c11", "-O2"],
      "xcode_settings": {
        "OTHER_CFLAGS": ["-std=c11", "-O2"],
        "MACOSX_DEPLOYMENT_TARGET": "10.15"
      },
      "msvs_settings": {
        "VCCLCompilerTool": {}
      },
      "conditions": [
        [
          "OS=='win'",
          {
            "libraries": []
          }
        ],
        [
          "OS!='win'",
          {
            "libraries": ["-ldl"]
          }
        ]
      ]
    }
  ]
}
