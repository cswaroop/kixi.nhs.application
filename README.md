kixi.nhs.application
====================

Dev setup:

1. In your home directory create a `.nhs.edn` with the contents below:
   ```edn
   {:ckan-client {:site "http://<site>/api/3/action/"
                  :api-key "<your_private_key>"}
    :schedule {:process-job-schedule
               ; s   m  h  d  M D
               {"0  36 11  *  * ?" {:dest :board-report :type :update :resource-id "68d5438a-e4d3-4be0-8e34-3ccd40930d"}}}
   }
   ```

2. Do `M-x cider-jack-in` in kixi.nhs.application project
3. Run `(go)`
4. Open up `dev/dev.clj` and try out some of the function there, e.g.
   `(dev/list-all-datasets system)` will print a list of *all*
   datasets in this client's CKAN

5. Try out some other functions:

  - `(take 10 (kixi.nhs.data.storage/get-resource-data (:ckan-client
    system) "0e73fe0d-0b16-4270-9026-f8fd8a75e684"))` to list 10
    values from that resource.

6. To create a new board report resource:

   ```clojure
   (use 'kixi.nhs.board-report)
   (insert-board-report-dataset (:ckan-client system) "resources/staging_config.edn")
   ```

7. To update an existing board report resource:

   ```clojure
   (update-board-report-dataset (:ckan-client system) "eb600f7a-95cb-4de4-add4-62c687fe0958" "resources/staging_config.edn")
   ```
