CREATE 
    ALGORITHM = UNDEFINED 
    DEFINER = `wiis`@`%` 
    SQL SECURITY DEFINER
VIEW `w_task_to_hl7_export_form` AS
    SELECT 
        `study`.`id` AS `study_id`,
        `study`.`accession_number` AS `study_accession_number`,
        `study`.`scheduled_datetime` AS `study_scheduled_datetime`,
        `study`.`modality_code` AS `study_modality_code`,
        `study`.`study_type_code` AS `study_study_type_code`,
        `study`.`location_code` AS `study_location_code`,
        `study`.`study_status_code` AS `study_study_status_code`,
        `study`.`created_at` AS `study_created_at`,
        `study`.`updated_at` AS `study_updated_at`,
        `task`.`id` AS `task_id`,
        `task`.`action` AS `task_action`,
        `task`.`parameters` AS `task_parameters`,
        `task`.`status` AS `task_status`,
        `task`.`created_at` AS `task_created_at`,
        `task`.`updated_at` AS `task_updated_at`,
        `patient`.`id` AS `patient_id`,
        `patient`.`hl7_patient_id` AS `patient_hl7_patient_id`,
        `patient`.`hl7_patient_patient_id` AS `patient_hl7_patient_patient_id`,
        `patient`.`ticket_number` AS `patient_ticket_number`,
        `patient`.`admit_datetime` AS `patient_admit_datetime`,
        `patient`.`admit_borne_code` AS `patient_admit_borne_code`,
        `patient`.`reason_of_visit_code` AS `patient_reason_of_visit_code`,
        (SELECT 
                `reason_of_visit`.`managerLabel`
            FROM
                `reason_of_visit`
            WHERE
                (`reason_of_visit`.`code` = `patient`.`reason_of_visit_code`)) AS `patient_reason_of_visit_label`,
        `patient`.`last_name` AS `patient_last_name`,
        `patient`.`first_name` AS `patient_first_name`,
        `patient`.`first_name_2` AS `patient_first_name_2`,
        `patient`.`first_name_3` AS `patient_first_name_3`,
        `patient`.`first_name_4` AS `patient_first_name_4`,
        `patient`.`birth_name` AS `patient_birth_name`,
        `patient`.`sex` AS `patient_sex`,
        `patient`.`birthdate` AS `patient_birthdate`,
        `patient`.`street_address` AS `patient_street_address`,
        `patient`.`city` AS `patient_city`,
        `patient`.`state` AS `patient_state`,
        `patient`.`zip_code` AS `patient_zip_code`,
        `patient`.`private_phone_number` AS `patient_private_phone_number`,
        `patient`.`phone_number` AS `patient_phone_number`,
        `patient`.`business_phone_number` AS `patient_business_phone_number`,
        `patient`.`mail` AS `patient_mail`,
        `patient`.`ssn_number` AS `patient_ssn_number`,
        `patient`.`location_code` AS `patient_location_code`,
        `patient`.`patient_status_code` AS `patient_patient_status_code`,
        `patient`.`identity_reliability_code` AS `patient_identity_reliability_code`,
        `patient`.`put_on_hold` AS `patient_put_on_hold`,
        `patient`.`last_called_at` AS `patient_last_called_at`,
        `patient`.`number_of_call` AS `patient_number_of_call`,
        `patient`.`created_at` AS `patient_created_at`,
        `patient`.`updated_at` AS `patient_updated_at`,
        `document`.`id` AS `document_id`,
        `document`.`patient_id` AS `document_patient_id`,
        `document`.`study_id` AS `document_study_id`,
        `document`.`name` AS `document_name`,
        `document`.`file_path` AS `document_file_path`,
        `document`.`alternative_file_path` AS `document_alternative_file_path`,
        `document`.`document_type_code` AS `document_type_code`,
        `document_type`.`name` AS `document_type_name`,
        `document_type`.`type` AS `document_type_level`,
        `document`.`created_at` AS `document_created_at`,
        `document`.`updated_at` AS `document_updated_at`,
        `form_filled`.`id` AS `form_filled_id`,
        `hl7_server`.`id` AS `hl7_server_id`,
        `hl7_server`.`application` AS `hl7_server_application`,
        `hl7_server`.`facility` AS `hl7_server_facility`,
        `hl7_server`.`ip` AS `hl7_server_ip`,
        `hl7_server`.`port` AS `hl7_server_port`,
        `visit`.`preadmit_number` AS `visit_preadmit_number`,
        `visit`.`admit_number` AS `visit_admit_number`,
        `visit`.`visit_description` AS `visit_description`,
        `visit`.`patient_class` AS `visit_patient_class`,
        IF(ISNULL(`patient_identifier`.`external_id`),
            CONVERT( CONCAT(`patient`.`id`, '^^^', 'BEA', '^', 'PI') USING UTF8),
            IF((`patient`.`identity_reliability_code` = 'VALI'),
                GROUP_CONCAT(CONCAT(`patient_identifier`.`external_id`,
                            '^^^',
                            `patient_identifier`.`assigning_authority`,
                            '&',
                            `patient_identifier`.`oid`,
                            '&',
                            `patient_identifier`.`universal_id_type`,
                            '^',
                            `patient_identifier`.`identifier_type_code`)
                    ORDER BY 1 ASC
                    SEPARATOR '~'),
                GROUP_CONCAT(CONCAT(`patient_identifier`.`external_id`,
                            '^^^',
                            `patient_identifier`.`assigning_authority`,
                            '^',
                            `patient_identifier`.`identifier_type_code`)
                    ORDER BY 1 ASC
                    SEPARATOR '~'))) AS `identifiant_hl7`
    FROM
        ((((((((((`hl7_server`
        JOIN `task`)
        JOIN `form_filled` ON ((`task`.`parameters` = `form_filled`.`id`)))
        JOIN `form` ON (((`form`.`code` = `form_filled`.`form_code`)
            AND (`form`.`export` = 1))))
        JOIN `workflow_history` ON ((`workflow_history`.`id` = `form_filled`.`workflow_history_id`)))
        JOIN `patient` ON ((`workflow_history`.`patient_id` = `patient`.`id`)))
        JOIN `patient_identifier` ON ((`patient_identifier`.`patient_id` = `patient`.`id`)))
        LEFT JOIN `study` ON (((`study`.`id` = `form_filled`.`study_id`)
            AND (`study`.`study_status_code` NOT IN ('CANCELED' , 'DONE')))))
        LEFT JOIN `visit` ON (((`visit`.`patient_id` = `study`.`patient_id`)
            AND (`visit`.`id` = `study`.`visit_id`))))
        LEFT JOIN `document` ON (((`document`.`patient_id` = `patient`.`id`)
            AND (`document`.`study_id` = `form_filled`.`study_id`)
            AND (`document`.`document_type_code` = `form`.`document_type_code`))))
        LEFT JOIN `document_type` ON ((`document_type`.`code` = `document`.`document_type_code`)))
    WHERE
        ((`task`.`status` = 'TODO')
            AND (`task`.`action` = 'EXPORT_FORM')
            AND (NOT (EXISTS( SELECT 
                'N'
            FROM
                `hl7_message_send` `s`
            WHERE
                (`s`.`task_id` = `task`.`id`)))))
    GROUP BY `study_id` , `study_accession_number` , `study_scheduled_datetime` , `study_modality_code` , `study_study_type_code` , `study_location_code` , `study_study_status_code` , `study_created_at` , `study_updated_at` , `task_id` , `task_action` , `task_parameters` , `task_status` , `task_created_at` , `task_updated_at` , `patient_id` , `patient_hl7_patient_id` , `patient_hl7_patient_patient_id` , `patient_ticket_number` , `patient_admit_datetime` , `patient_admit_borne_code` , `patient_reason_of_visit_code` , `patient_last_name` , `patient_first_name` , `patient_sex` , `patient_birthdate` , `patient_street_address` , `patient_city` , `patient_state` , `patient_zip_code` , `patient_private_phone_number` , `patient_business_phone_number` , `patient_mail` , `patient_ssn_number` , `patient_location_code` , `patient_patient_status_code` , `patient_put_on_hold` , `patient_last_called_at` , `patient_number_of_call` , `patient_created_at` , `patient_updated_at` , `document_id` , `document_patient_id` , `document_study_id` , `document_name` , `document_file_path` , `document_alternative_file_path` , `document`.`document_type_code` , `document_type_name` , `document_type_level` , `document_created_at` , `document_updated_at` , `form_filled_id` , `hl7_server_id` , `hl7_server_application` , `hl7_server_facility` , `hl7_server_ip` , `hl7_server_port` , `visit_admit_number` , `visit_preadmit_number`